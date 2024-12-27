package hraft

import (
	"fmt"
	"sync/atomic"
)

func (r *Raft) handleAppendEntries(rpc *RPC, req *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		Term:          r.getTerm(),
		LastLogIdx:    r.instate.getLastIdx(),
		Success:       false,
		PrevLogFailed: false,
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
		resp.PrevLogFailed = true
		return
	}
	if err := r.appendEntries(req.Entries); err != nil {
		return
	}
	r.updateLeaderCommit(req.LeaderCommit)
	resp.Success = true
	r.heartbeatTimeout.reset()
}

func (r *Raft) checkPrevLog(idx, term uint64) bool {
	if idx == 0 {
		return true
	}
	lastIdx, lastTerm := r.instate.getLastLog()
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

	lastLogIdx, _ := r.instate.getLastLog()
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
			// clear log suffix
			r.logger.Warn("clearing logs suffix",
				"from", entry.Idx,
				"to", lastLogIdx)
			if err := r.logs.DeleteRange(entry.Idx, lastLogIdx); err != nil {
				r.logger.Error("failed to clear log suffix", "error", err)
				return err
			}
			// update last-log
			lastLogIdx, err := r.logs.LastIdx()
			if err != nil {
				r.logger.Warn("failed to get last-log-index from store", "error", err)
				return err
			}
			if err := r.logs.GetLog(lastLogIdx, existed); err != nil {
				r.logger.Warn("failed to get log entry",
					"index", lastLogIdx,
					"error", err)
				return err
			}
			r.instate.setLastLog(lastLogIdx, existed.Term)
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
	r.handleNewLeaderCommit(idx)

}

type Commit struct {
	Log   *Log
	ErrCh chan error
}

func (r *Raft) handleNewLeaderCommit(idx uint64) {
	lastApplied := r.instate.getLastApplied()
	if idx <= lastApplied {
		r.logger.Warn("skipping application of old log", "index", idx)
		return
	}
	batchSize := r.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)
	for i := lastApplied; i <= idx; i++ {
		l := &Log{}
		if err := r.logs.GetLog(i, l); err != nil {
			r.logger.Error("failed to get log", "index", i, "error", err)
			panic(err)
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
			trySendErr(c.ErrCh, ErrRaftShutdown)
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

func (r *Raft) NumNodes() int {
	return 0
}

func (r *Raft) Peers() []string {
	return []string{}
}

func (r *Raft) ID() string {
	return "" // use address, bindAddr
}
