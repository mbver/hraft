package hraft

import (
	"errors"
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
	To   RaftStateType
	Term uint64
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

type Raft struct {
	config             *Config
	logger             hclog.Logger
	appstate           *AppState
	instate            *internalState
	state              RaftStateType
	stateMap           map[RaftStateType]State
	logs               *LogStore
	kvs                *KVStore
	heartbeatCh        chan *RPC
	rpchCh             chan *RPC
	applyCh            chan *Apply
	commitNotifyCh     chan struct{}
	transitionCh       chan *Transition
	resetHeartbeatCh   chan struct{}
	heartbeatTimeoutCh <-chan time.Time
	shutdown           *ProtectedChan
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

func (r *Raft) NumNodes() int {
	return 0
}

func (r *Raft) Peers() []string {
	return []string{}
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

// raft's mainloop
func (r *Raft) receiveMsgs() {
	for {
		transition := r.getTransition()
		r.getState().HandleTransition(transition)

		if r.getResetHeartbeatTimeout() {
			r.heartbeatTimeoutCh = time.After(r.config.HeartbeatTimeout)
		}
		select {
		case rpc := <-r.rpchCh:
			r.getState().HandleRPC(rpc)
		case apply := <-r.applyCh:
			r.getState().HandleApply(apply)
		case <-r.commitNotifyCh:
			r.getState().HandleCommitNotify()
		case <-r.heartbeatTimeoutCh:
			r.getState().HandleHeartbeatTimeout()
		}
	}
}

func (r *Raft) getTransition() *Transition {
	select {
	case transition := <-r.transitionCh:
		return transition
	default:
		return nil
	}
}

func (r *Raft) getResetHeartbeatTimeout() bool {
	select {
	case <-r.resetHeartbeatCh:
		return true
	default:
		return false
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
