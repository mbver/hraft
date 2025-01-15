package hraft

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
)

type RaftBuilder struct {
	config          *Config
	transportConfig *NetTransportConfig
	appState        *AppState
	logStore        LogStore
	kvStore         KVStore
	logger          hclog.Logger
}

func (b *RaftBuilder) WithConfig(c *Config) {
	b.config = c
}

func (b *RaftBuilder) WithTransportConfig(c *NetTransportConfig) {
	b.transportConfig = c
}

func (b *RaftBuilder) WithAppState(a *AppState) {
	b.appState = a
}

func (b *RaftBuilder) WithLogStore(s LogStore) {
	b.logStore = s
}

func (b *RaftBuilder) WithKVStore(kv KVStore) {
	b.kvStore = kv
}

func (b *RaftBuilder) WithLogger(logger hclog.Logger) {
	b.logger = logger
}

func (b *RaftBuilder) Build() (*Raft, error) {
	if !validateConfig(b.config) {
		return nil, fmt.Errorf("invalid config")
	}
	transport, err := NewNetTransport(b.transportConfig, b.logger)
	if err != nil {
		return nil, err
	}
	raft := &Raft{
		config:             b.config,
		logger:             b.logger,
		appstate:           b.appState,
		membership:         newMembership(b.config.LocalID),
		instate:            &internalState{},
		state:              followerStateType,
		logs:               b.logStore,
		kvs:                b.kvStore,
		transport:          transport,
		heartbeatCh:        transport.HeartbeatCh(),
		rpchCh:             transport.RpcCh(),
		applyCh:            make(chan *Apply),
		commitNotifyCh:     make(chan struct{}, 1),
		membershipChangeCh: make(chan *membershipChange),
		transitionCh:       make(chan *Transition), // buffer?
		heartbeatTimeout:   newHeartbeatTimeout(b.config.HeartbeatTimeout),
		wg:                 &ProtectedWaitGroup{},
		shutdown:           newProtectedChan(),
	}
	raft.stateMap = NewStateMap(raft)
	go raft.receiveMsgs()
	go raft.receiveHeartbeat()
	go raft.receiveTransitions()
	go raft.appstate.receiveMutations()

	return raft, nil
}

func NewStateMap(r *Raft) map[RaftStateType]State {
	m := map[RaftStateType]State{}
	m[followerStateType] = NewFollower(r)
	m[candidateStateType] = NewCandidate(r)
	m[leaderStateType] = NewLeader(r)
	return m
}
