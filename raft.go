package hraft

import hclog "github.com/hashicorp/go-hclog"

type RaftStateType uint8

const (
	followerStateType = iota
	candidateStateType
	leaderStateType
)

type State interface{}

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
	config   *Config
	state    RaftStateType
	stateMap map[RaftStateType]State
	shutdown *ProtectedChan
}

func NewStateMap(r *Raft) map[RaftStateType]State {
	m := map[RaftStateType]State{}
	m[followerStateType] = NewFollower(r)
	m[candidateStateType] = NewCandidate(r)
	m[leaderStateType] = NewLeader(r)
	return m
}

func (r *Raft) ShutdownCh() chan struct{} {
	return r.shutdown.Ch()
}

func (r *Raft) getState() State {
	return r.stateMap[r.state]
}

func (r *Raft) getLeaderState() *Leader {
	return r.stateMap[leaderStateType].(*Leader)
}

func (r *Raft) NumNodes() int {
	return 0
}

func (r *Raft) getCommitIdx() uint64 {
	return 0
}

func (r *Raft) setCommitIdx(idx uint64) {}

func (r *Raft) processNewLeaderCommit(idx uint64) {}

func (r *Raft) applyCommits(commits []*Commit) {}
