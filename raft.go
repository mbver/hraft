package hraft

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

var (
	ErrRaftShutdown = errors.New("raft is shutdown")
)

type RaftStateType uint32

const (
	followerStateType = iota
	candidateStateType
	leaderStateType
)

type Transition struct {
	To     RaftStateType
	Term   uint64
	DoneCh chan struct{}
}

func newTransition(to RaftStateType, term uint64) *Transition {
	return &Transition{
		To:     to,
		Term:   term,
		DoneCh: make(chan struct{}),
	}
}

type State interface {
	HandleTransition(*Transition)
	HandleHeartbeatTimeout()
	HandleRPC(*RPC)
	HandleApply(*Apply)
	HandleCommitNotify()
}

type RaftBuilder struct {
	config   *Config
	appState *AppState
	logStore *LogStore
	kvStore  *KVStore
	logger   hclog.Logger
}

func (b *RaftBuilder) WithConfig(c *Config) {
	b.config = c
}

func (b *RaftBuilder) WithAppState(a *AppState) {
	b.appState = a
}

func (b *RaftBuilder) WithLogStore(s *LogStore) {
	b.logStore = s
}

func (b *RaftBuilder) WithKVStore(kv *KVStore) {
	b.kvStore = kv
}

func (b *RaftBuilder) WithLogger(logger hclog.Logger) {
	b.logger = logger
}

type heartbeatTimeout struct {
	timeout time.Duration
	l       sync.Mutex
	ch      <-chan time.Time
	fresh   bool
}

func newHeartbeatTimeout(timeout time.Duration) *heartbeatTimeout {
	return &heartbeatTimeout{
		timeout: timeout,
		ch:      jitterTimeoutCh(timeout),
		fresh:   true,
	}
}

func (h *heartbeatTimeout) reset() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = jitterTimeoutCh(h.timeout)
	h.fresh = true
}

func (h *heartbeatTimeout) block() {
	h.l.Lock()
	defer h.l.Unlock()
	h.ch = nil
	h.fresh = true
}

func (h *heartbeatTimeout) getCh() <-chan time.Time {
	h.l.Lock()
	defer h.l.Unlock()
	h.fresh = false
	return h.ch
}

func (h *heartbeatTimeout) isFresh() bool {
	h.l.Lock()
	defer h.l.Unlock()
	return h.fresh
}

type Raft struct {
	config           *Config
	logger           hclog.Logger
	appstate         *AppState
	instate          *internalState
	state            RaftStateType
	stateMap         map[RaftStateType]State
	logs             *LogStore
	kvs              *KVStore
	transport        *netTransport
	heartbeatCh      chan *RPC
	rpchCh           chan *RPC
	applyCh          chan *Apply
	commitNotifyCh   chan struct{}
	transitionCh     chan *Transition
	heartbeatTimeout *heartbeatTimeout
	shutdown         *ProtectedChan
}

func NewStateMap(r *Raft) map[RaftStateType]State {
	m := map[RaftStateType]State{}
	m[followerStateType] = NewFollower(r)
	m[candidateStateType] = NewCandidate(r)
	m[leaderStateType] = NewLeader(r)
	return m
}

func (r *Raft) ID() string {
	return "" // use address, bindAddr
}

func (r *Raft) ShutdownCh() chan struct{} {
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

func (r *Raft) NumNodes() int {
	return 0
}

func (r *Raft) Peers() []string {
	return []string{}
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

type Commit struct {
	Log   *Log
	ErrCh chan error
}

func (r *Raft) processNewLeaderCommit(idx uint64) {
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

func (r *Raft) dispatchTransition(to RaftStateType, term uint64) chan struct{} {
	transition := newTransition(to, term)
	r.transitionCh <- transition
	return transition.DoneCh
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
	r.processNewLeaderCommit(idx)

}

func (r *Raft) handleAppendEntries(rpc *RPC, req *AppendEntriesRequest) {
	resp := &AppendEntriesResponse{
		Term:          r.instate.getTerm(),
		LastLogIdx:    r.instate.getLastIdx(),
		Success:       false,
		PrevLogFailed: false,
	}
	defer func() {
		rpc.respCh <- resp
	}()
	if req.Term < r.instate.getTerm() {
		return
	}
	if req.Term > r.instate.getTerm() {
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

// raft's mainloop
func (r *Raft) receiveMsgs() {
	for {
		select {
		case rpc := <-r.rpchCh:
			r.getState().HandleRPC(rpc)
		case apply := <-r.applyCh:
			r.getState().HandleApply(apply)
		case <-r.commitNotifyCh:
			r.getState().HandleCommitNotify()
		case <-r.heartbeatTimeout.getCh():
			if r.heartbeatTimeout.isFresh() { // heartTimeout is reset, keep going
				continue
			}
			r.getState().HandleHeartbeatTimeout()
		case <-r.ShutdownCh():
			return
		}
	}
}

func (r *Raft) receiveHeartbeat() {
	for {
		select {
		case req := <-r.heartbeatCh:
			r.getState().HandleRPC(req)
		case <-r.ShutdownCh():
			return
		}
	}
}

func (r *Raft) receiveTransitions() {
	for {
		select {
		case transition := <-r.transitionCh:
			r.getState().HandleTransition(transition)
			close(transition.DoneCh)
		case <-r.ShutdownCh():
			return
		}
	}
}

func (r *Raft) receiveMutations() {
	select {
	case commits := <-r.appstate.mutateCh:
		r.appstate.state.Apply(commits)
	case <-r.ShutdownCh():
		return
	}
}
