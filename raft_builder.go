package hraft

import hclog "github.com/hashicorp/go-hclog"

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

func (b *RaftBuilder) Build() (*Raft, error) {
	return nil, nil
}

func NewStateMap(r *Raft) map[RaftStateType]State {
	m := map[RaftStateType]State{}
	m[followerStateType] = NewFollower(r)
	m[candidateStateType] = NewCandidate(r)
	m[leaderStateType] = NewLeader(r)
	return m
}
