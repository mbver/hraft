package hraft

import (
	"errors"
	"fmt"
	"io"
	"sync"

	hclog "github.com/hashicorp/go-hclog"
)

var (
	ErrRaftShutdown = errors.New("raft is shutdown")
)

type RaftStateType uint32

const (
	followerStateType RaftStateType = iota
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
	HandleRestoreRequest(*userRestoreRequest)
	HandleLeadershipTransfer(*leadershipTransfer)
}

type Raft struct {
	config               *Config
	logger               hclog.Logger
	appstate             *AppState
	membership           *membership
	instate              *internalState
	state                RaftStateType
	stateMap             map[RaftStateType]State
	logs                 LogStore
	kvs                  KVStore
	snapstore            *SnapshotStore
	transport            *NetTransport
	heartbeatCh          chan *RPC
	rpchCh               chan *RPC
	applyCh              chan *Apply
	commitNotifyCh       chan struct{}
	membershipChangeCh   chan *membershipChange
	leadershipTransferCh chan *leadershipTransfer
	transitionCh         chan *Transition
	snapshotReqCh        chan *userSnapshotRequest
	restoreReqCh         chan *userRestoreRequest
	heartbeatTimeout     *heartbeatTimeout
	wg                   *ProtectedWaitGroup
	shutdown             *ProtectedChan
}

func (r *Raft) Shutdown() {
	if r.shutdown.IsClosed() {
		return
	}
	r.shutdown.Close()
	r.wg.Wait()
	r.appstate.Stop()
	r.transport.Close()
}

// raft's mainloop
func (r *Raft) receiveMsgs() {
	r.wg.Add(1)
	defer r.wg.Done()
	warnOnce := sync.Once{}
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
		case transfer := <-r.leadershipTransferCh:
			r.getState().HandleLeadershipTransfer(transfer)
		case req := <-r.restoreReqCh:
			r.getState().HandleRestoreRequest(req)
		case <-r.heartbeatTimeout.getCh():
			if !r.membership.isActive() || !r.membership.isLocalVoter() {
				r.heartbeatTimeout.reset()
				warnOnce.Do(func() { r.logger.Warn("membership is unactivated or local node is not voter, skipping election") })
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
			r.logger.Info("receive transition msg",
				"transition", transition.String(),
				"current_state", r.getStateType().String(),
				"current_term", r.getTerm(),
			)
			r.getState().HandleTransition(transition)
			close(transition.DoneCh)
		case <-r.shutdownCh():
			return
		}
	}
}

func (r *Raft) receiveSnapshotRequests() {
	r.wg.Add(1)
	defer r.wg.Done()
	for {
		select {
		case req := <-r.snapshotReqCh:
			name, err := r.takeSnapshot()
			if err == nil {
				req.openSnapshot = func() (*SnapshotMeta, io.ReadCloser, error) {
					return r.snapstore.OpenSnapshot(name)
				}
			}
			req.errCh <- err
		case <-jitterTimeoutCh((r.config.SnapshotInterval)):
			if !r.shouldSnapshot() {
				continue
			}
			if _, err := r.takeSnapshot(); err != nil {
				r.logger.Error("failed to take snapshot", "error", err)
			}
		// skip user trigger snapshot for now
		case <-r.shutdownCh():
			return
		}
	}
}
