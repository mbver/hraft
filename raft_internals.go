package hraft

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

var (
	ErrNotLeader          = errors.New("not leader")
	ErrTimeout            = errors.New("timeout")
	ErrMembershipUnstable = errors.New("membership is unstable")
)

func (r *Raft) handleRPC(rpc *RPC) {
	switch req := rpc.command.(type) {
	case *AppendEntriesRequest:
		r.handleAppendEntries(rpc, req)
	case *VoteRequest:
		r.handleRequestVote(rpc, req)
	default:

	}
}

func (r *Raft) handleAppendEntries(rpc *RPC, req *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		Term:               r.getTerm(),
		LastLogIdx:         r.instate.getLastIdx(),
		Success:            false,
		PrevLogCheckFailed: false,
	}
	defer func() {
		rpc.respCh <- resp
	}()
	if req.Term < r.getTerm() {
		return
	}
	if req.Term > r.getTerm() {
		waitCh := r.dispatchTransition(followerStateType, req.Term)
		<-waitCh
		resp.Term = req.Term
	}
	if !r.checkPrevLog(req.PrevLogIdx, req.PrevLogTerm) {
		resp.PrevLogCheckFailed = true
		return
	}
	if err := r.appendEntries(req.Entries); err != nil {
		return
	}
	r.updateLeaderCommit(req.LeaderCommitIdx)
	resp.Success = true
	r.heartbeatTimeout.reset()
}

func (r *Raft) checkPrevLog(idx, term uint64) bool {
	if idx == 0 {
		return true
	}
	lastIdx, lastTerm := r.getLastLog()
	if lastIdx == idx {
		return term == lastTerm
	}
	prevLog := &Log{}
	if err := r.logs.GetLog(idx, prevLog); err != nil {
		r.logger.Warn("failed to get previous log",
			"previous-index", idx,
			"last-index", lastIdx,
			"error", err)
		return false
	}
	if prevLog.Term != term {
		r.logger.Warn("previous log term mis-match",
			"ours", prevLog.Term,
			"remote", term)
		return false
	}
	return true
}

func (r *Raft) appendEntries(entries []*Log) error {
	if len(entries) == 0 {
		return nil
	}

	lastLogIdx, _ := r.getLastLog()
	var newEntries []*Log
	for i, entry := range entries {
		if entry.Idx > lastLogIdx {
			newEntries = entries[i:]
			break
		}
		existed := &Log{}
		if err := r.logs.GetLog(entry.Idx, existed); err != nil {
			r.logger.Warn("failed to get log entry",
				"index", entry.Idx,
				"error", err)
			return err
		}
		// check for log inconsitencies and handle it
		if entry.Term != existed.Term {
			// clear log suffix, last log updates will be done later
			r.logger.Warn("clearing logs suffix",
				"from", entry.Idx,
				"to", lastLogIdx)
			if err := r.logs.DeleteRange(entry.Idx, lastLogIdx); err != nil {
				r.logger.Error("failed to clear log suffix", "error", err)
				return err
			}
			if _, latestIdx := r.membership.getLatest(); latestIdx <= entry.Idx {
				r.membership.rollbackToCommitted()
			}
			// get new-entries
			newEntries = entries[i:]
			break
		}
	}
	if len(newEntries) == 0 {
		return nil
	}
	if err := r.logs.StoreLogs(newEntries); err != nil {
		r.logger.Error("failed to append to logs", "error", err)
		return err
	}
	for _, log := range newEntries {
		if log.Type == LogMembership {
			// this might look redundant but absolutely necessary
			r.membership.setCommitted(r.membership.getLatest())
			var peers []*Peer
			if err := decode(log.Data, &peers); err != nil {
				return err
			}
			r.membership.setLatest(peers, log.Idx)
		}
	}
	last := newEntries[len(newEntries)-1]
	r.instate.setLastLog(last.Idx, last.Term)
	return nil
}

func (r *Raft) updateLeaderCommit(idx uint64) {
	if idx == 0 || idx <= r.instate.getCommitIdx() {
		return
	}
	idx = min(idx, r.instate.getLastIdx())
	r.instate.setCommitIdx(idx)
	if _, latestIdx := r.membership.getLatest(); latestIdx <= idx {
		r.membership.setCommitted(r.membership.getLatest())
	}
	r.handleNewLeaderCommit(idx)
}

type Commit struct {
	Log   *Log
	ErrCh chan error
}

func (r *Raft) handleNewLeaderCommit(idx uint64) {
	lastApplied := r.instate.getLastApplied()
	if idx <= lastApplied {
		r.logger.Warn("ignore old applied logs", "idx", idx)
		return
	}
	batchSize := r.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)
	for i := lastApplied + 1; i <= idx; i++ {
		l := &Log{}
		if err := r.logs.GetLog(i, l); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			panic(err)
		}
		// no-op log is skipped
		if l.Type == LogNoOp {
			continue
		}
		batch = append(batch, &Commit{l, nil})
		if len(batch) == batchSize {
			r.applyCommits(batch)
			batch = make([]*Commit, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		r.applyCommits(batch)
	}
	r.instate.setLastApplied(idx)
}

func (r *Raft) applyCommits(commits []*Commit) {
	select {
	case r.appstate.mutateCh <- commits:
	case <-r.shutdown.Ch():
		for _, c := range commits {
			trySend(c.ErrCh, ErrRaftShutdown)
		}
	}
}

func (r *Raft) getPrevLog(nextIdx uint64) (idx, term uint64, err error) {
	if nextIdx == 1 {
		return 0, 0, nil
	}
	// skip snapshot stuffs for now
	l := &Log{}
	if err = r.logs.GetLog(nextIdx-1, l); err != nil {
		return 0, 0, err
	}
	return l.Idx, l.Term, nil
}

func (r *Raft) getEntries(nextIdx, uptoIdx uint64) ([]*Log, error) {
	size := r.config.MaxAppendEntries
	entries := make([]*Log, 0, size)
	maxIdx := min(nextIdx-1+uint64(size), uptoIdx)
	for i := nextIdx; i <= maxIdx; i++ {
		l := &Log{}
		if err := r.logs.GetLog(i, l); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			return nil, err
		}
		entries = append(entries, l)
	}
	return entries, nil
}

func (r *Raft) dispatchTransition(to RaftStateType, term uint64) chan struct{} {
	transition := newTransition(to, term)
	r.transitionCh <- transition
	return transition.DoneCh
}

func (r *Raft) shutdownCh() chan struct{} {
	return r.shutdown.Ch()
}

func (r *Raft) getStateType() RaftStateType {
	s := atomic.LoadUint32((*uint32)(&r.state))
	return RaftStateType(s)
}

func (r *Raft) setStateType(s RaftStateType) {
	atomic.StoreUint32((*uint32)(&r.state), uint32(s))
}

func (r *Raft) getState() State {
	return r.stateMap[r.getStateType()]
}

func (r *Raft) getLeaderState() *Leader {
	return r.stateMap[leaderStateType].(*Leader)
}

func (r *Raft) getCandidateState() *Candidate {
	return r.stateMap[candidateStateType].(*Candidate)
}

var (
	keyCurrentTerm  = []byte("CurrentTerm")
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
)

func (r *Raft) getTerm() uint64 {
	return r.instate.getTerm()
}

func (r *Raft) setTerm(term uint64) {
	if err := r.kvs.SetUint64(keyCurrentTerm, term); err != nil {
		panic(fmt.Errorf("failed to save term: %v", err))
	}
	r.instate.setTerm(term)
}

func (r *Raft) getLastLog() (uint64, uint64) {
	return r.instate.getLastLog()
}

func (r *Raft) setLastLog(idx, term uint64) {
	r.instate.setLastLog(idx, term)
}

func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.kvs.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.kvs.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

func (r *Raft) handleRequestVote(rpc *RPC, req *VoteRequest) {
	if !r.membership.isLocalVoter() { // non-voter node don't involve request vote
		return
	}
	// setup a response
	resp := &VoteResponse{
		Term:    r.getTerm(),
		Granted: false,
	}
	defer func() {
		rpc.respCh <- resp
	}()
	// ignore an older term
	if req.Term < r.getTerm() {
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getTerm() {
		waitCh := r.dispatchTransition(followerStateType, req.Term)
		<-waitCh
		resp.Term = req.Term
	}
	if r.getStateType() != followerStateType { // don't grant vote if we are candidate/leader
		return
	}

	candidate := req.Candidate
	// Check if we have voted
	lastVoteTerm, err := r.kvs.GetUint64(keyLastVoteTerm)
	if err != nil && !errors.Is(err, ErrKeyNotFound) { // it's ok if we haven't voted
		r.logger.Error("failed to get last vote term", "error", err)
		return
	}
	lastCandidate, err := r.kvs.Get(keyLastVoteCand)
	if err != nil && !errors.Is(err, ErrKeyNotFound) { // it's ok if we haven't voted
		r.logger.Error("failed to get last vote candidate", "error", err)
		return
	}

	// If a node is granted vote but fails to receive by network failures,
	// it should receive that in next election (of the same term).
	// When we voted for a node, we will not transition to candidate in that term.
	if lastVoteTerm == req.Term {
		r.logger.Info("duplicate requestVote for same term", "term", req.Term)
		if bytes.Equal(candidate, lastCandidate) {
			r.logger.Warn("duplicate requestVote from", "candidate", candidate)
			resp.Granted = true
		}
		return
	}

	lastIdx, lastTerm := r.instate.getLastLog()
	// reject older last log term
	if lastTerm > req.LastLogTerm {
		r.logger.Warn("rejecting vote request since our last term is greater",
			"candidate", candidate,
			"last-term", lastTerm,
			"last-candidate-term", req.LastLogTerm)
		return
	}
	// reject older last logIdx
	if lastTerm == req.LastLogTerm && lastIdx > req.LastLogIdx {
		r.logger.Warn("rejecting vote request since our last index is greater",
			"candidate", candidate,
			"last-index", lastIdx,
			"last-candidate-index", req.LastLogIdx)
		return
	}

	// Persist a vote in case it fails to receive and ask again.
	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logger.Error("failed to persist vote", "error", err)
		return
	}

	resp.Granted = true
	r.heartbeatTimeout.reset()
}

func (r *Raft) hasExistingState() (bool, error) {
	_, err := r.kvs.GetUint64(keyCurrentTerm)
	if err != ErrKeyNotFound {
		if err == nil {
			return true, nil
		}
		return false, err
	}
	lastIdx, err := r.logs.LastIdx()
	if err != nil {
		return false, err
	}
	if lastIdx > 0 {
		return true, nil
	}
	return false, nil
}

func sendToRaft[T *Apply | *membershipChange](ch chan T, msg T, timeoutCh <-chan time.Time, shutdownCh chan struct{}) error {
	select {
	case ch <- msg:
		return nil
	case <-timeoutCh:
		return fmt.Errorf("timeout sending to raft")
	case <-shutdownCh:
		return ErrRaftShutdown
	}
}

func drainErr(errCh chan error, timeoutCh <-chan time.Time, shutdownCh chan struct{}) error {
	select {
	case err := <-errCh:
		return err
	case <-timeoutCh:
		return fmt.Errorf("timeout draining error")
	case <-shutdownCh:
		return ErrRaftShutdown
	}
}
