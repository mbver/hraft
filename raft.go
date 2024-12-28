package hraft

import (
	"errors"

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

type Raft struct {
	config           *Config
	logger           hclog.Logger
	appstate         *AppState
	membership       *membership
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
		case <-r.shutdownCh():
			return
		}
	}
}

// fast path for heartbeat msgs
func (r *Raft) receiveHeartbeat() {
	for {
		select {
		case req := <-r.heartbeatCh:
			r.getState().HandleRPC(req)
		case <-r.shutdownCh():
			return
		}
	}
}

// handle state transition
func (r *Raft) receiveTransitions() {
	for {
		select {
		case transition := <-r.transitionCh:
			r.getState().HandleTransition(transition)
			close(transition.DoneCh)
		case <-r.shutdownCh():
			return
		}
	}
}

// handle commits to apply on app state machine
func (r *Raft) receiveMutations() {
	select {
	case commits := <-r.appstate.mutateCh:
		r.appstate.state.Apply(commits)
	case <-r.shutdownCh():
		return
	}
}
