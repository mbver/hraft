package hraft

import (
	"fmt"

	hclog "github.com/hashicorp/go-hclog"
)

type RaftBuilder struct {
	config          *Config
	transportConfig *NetTransportConfig
	connGetter      ConnGetter
	appState        *AppState
	logStore        LogStore
	kvStore         KVStore
	snapStore       *SnapshotStore
	logger          hclog.Logger
}

func (b *RaftBuilder) WithConfig(c *Config) {
	b.config = c
}

func (b *RaftBuilder) WithTransportConfig(c *NetTransportConfig) {
	b.transportConfig = c
}

func (b *RaftBuilder) WithConnGetter(g ConnGetter) {
	b.connGetter = g
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

func (b *RaftBuilder) WithSnapStore(s *SnapshotStore) {
	b.snapStore = s
}

func (b *RaftBuilder) WithLogger(logger hclog.Logger) {
	b.logger = logger
}

func (b *RaftBuilder) Build() (*Raft, error) {
	if !validateConfig(b.config) {
		return nil, fmt.Errorf("invalid config")
	}

	trans, err := NewNetTransport(b.transportConfig, b.logger, b.connGetter)
	if err != nil {
		return nil, err
	}

	raft := &Raft{
		config:               b.config,
		logger:               b.logger,
		appstate:             b.appState,
		membership:           newMembership(b.config.LocalID),
		instate:              newInternalState(),
		state:                followerStateType,
		logs:                 b.logStore,
		kvs:                  b.kvStore,
		snapstore:            b.snapStore,
		transport:            trans,
		heartbeatCh:          trans.HeartbeatCh(),
		rpchCh:               trans.RpcCh(),
		applyCh:              make(chan *Apply, b.config.MaxAppendEntries),
		commitNotifyCh:       make(chan struct{}, 1),
		membershipChangeCh:   make(chan *membershipChange),
		leadershipTransferCh: make(chan *leadershipTransfer),
		leaderContact:        newContactTime(),
		verifyLeaderCh:       make(chan *verifyLeaderRequest),
		transitionCh:         make(chan *Transition),
		snapshotReqCh:        make(chan *userSnapshotRequest),
		restoreReqCh:         make(chan *userRestoreRequest),
		heartbeatTimeout:     newHeartbeatTimeout(b.config.HeartbeatTimeout),
		observers:            newObserverManager(),
		wg:                   &ProtectedWaitGroup{},
		shutdown:             newProtectedChan(),
		stopTransitionCh:     make(chan struct{}),
		transitionStopDoneCh: make(chan struct{}),
	}
	raft.stateMap = NewStateMap(raft)
	go raft.receiveMsgs()
	go raft.receiveHeartbeat()
	go raft.receiveTransitions()
	go raft.appstate.receiveMutations()
	go raft.receiveSnapshotRequests()

	return raft, nil
}

func NewStateMap(r *Raft) map[RaftStateType]State {
	m := map[RaftStateType]State{}
	m[followerStateType] = NewFollower(r)
	m[candidateStateType] = NewCandidate(r)
	m[leaderStateType] = NewLeader(r)
	return m
}
