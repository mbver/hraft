package hraft

import (
	"errors"
	"fmt"
	"sync"

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

func (t RaftStateType) String() string {
	switch t {
	case followerStateType:
		return "follower"
	case candidateStateType:
		return "candidate"
	case leaderStateType:
		return "leader"
	}
	return "unknown state type"
}

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

func (t *Transition) String() string {
	return fmt.Sprintf("{To: %s, Term: %d}", t.To, t.Term)
}

type State interface {
	HandleTransition(*Transition)
	HandleHeartbeatTimeout()
	HandleApply(*Apply)
	HandleCommitNotify()
	HandleMembershipChange(*membershipChange)
}

type Raft struct {
	config             *Config
	logger             hclog.Logger
	appstate           *AppState
	membership         *membership
	instate            *internalState
	state              RaftStateType
	stateMap           map[RaftStateType]State
	logs               LogStore
	kvs                KVStore
	transport          *netTransport
	heartbeatCh        chan *RPC
	rpchCh             chan *RPC
	applyCh            chan *Apply
	commitNotifyCh     chan struct{}
	membershipChangeCh chan *membershipChange
	transitionCh       chan *Transition
	heartbeatTimeout   *heartbeatTimeout
	wg                 sync.WaitGroup
	shutdown           *ProtectedChan
}

func (r *Raft) Shutdown() {
	r.shutdown.Close()
	r.wg.Wait()
	r.appstate.Stop()
	r.transport.Close()
}

// raft's mainloop
func (r *Raft) receiveMsgs() {
	r.wg.Add(1)
	defer r.wg.Done()
	for {
		select {
		case rpc := <-r.rpchCh:
			r.handleRPC(rpc)
		case apply := <-r.applyCh:
			r.getState().HandleApply(apply)
		case <-r.commitNotifyCh:
			r.getState().HandleCommitNotify()
		case change := <-r.membershipChangeCh:
			r.getState().HandleMembershipChange(change)
		case <-r.heartbeatTimeout.getCh():
			if !r.membership.getLocal().isVoter() { // non-voter node will not transition to candidate
				r.heartbeatTimeout.reset()
				continue
			}
			r.getState().HandleHeartbeatTimeout()
		case <-r.heartbeatTimeout.getResetNotifyCh():
			continue
		case <-r.shutdownCh():
			return
		}
	}
}

// fast path for heartbeat msgs
func (r *Raft) receiveHeartbeat() {
	r.wg.Add(1)
	defer r.wg.Done()
	for {
		select {
		case req := <-r.heartbeatCh:
			r.handleRPC(req)
		case <-r.shutdownCh():
			return
		}
	}
}

// handle state transition
func (r *Raft) receiveTransitions() {
	r.wg.Add(1)
	defer r.wg.Done()
	for {
		select {
		case transition := <-r.transitionCh:
			r.logger.Info("receive transition msg", "transition", transition.String())
			r.getState().HandleTransition(transition)
			close(transition.DoneCh)
		case <-r.shutdownCh():
			return
		}
	}
}
