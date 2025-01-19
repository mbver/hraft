package hraft

import (
	"fmt"
	"time"
)

func (r *Raft) NumNodes() int {
	return 0
}

func (r *Raft) Voters() []string {
	return r.membership.getVoters()
}

func (r *Raft) PeerAddresses() []string {
	return r.membership.peerAddresses()
}

func (r *Raft) ID() string {
	return r.membership.getLocalID()
}

func (r *Raft) StagingPeer() string {
	return r.membership.getStaging()
}

type Apply struct {
	log          *Log
	errCh        chan error
	dispatchedAt time.Time
}

func newApply(cmd []byte) *Apply {
	return &Apply{
		log: &Log{
			Type: LogCommand,
			Data: cmd,
		},
		errCh: make(chan error, 1),
	}
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) error {
	a := newApply(cmd)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.applyCh, a, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(a.errCh, timeoutCh, r.shutdownCh())
}

func (r *Raft) AddVoter(addr string, timeout time.Duration) error {
	m := newMembershipChange(addr, addStaging)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.membershipChangeCh, m, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(m.errCh, timeoutCh, r.shutdownCh())
}

func (r *Raft) RemovePeer(addr string, timeout time.Duration) error {
	m := newMembershipChange(addr, removePeer)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.membershipChangeCh, m, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(m.errCh, timeoutCh, r.shutdownCh())
}

// Bootstrap is called only once on the first node in a cluster.
// Subsequent calls will return an error that can be safely ignored.
// Later nodes are not bootstraped and added via AddVoter.
func (r *Raft) Bootstrap(timeout time.Duration) error { // TODO: timeout is in config?
	hasState, err := r.hasExistingState()
	if err != nil {
		return err
	}
	if hasState {
		return fmt.Errorf("has existing state, can't bootstrap")
	}
	peers := []*Peer{{r.ID(), RoleVoter}}
	r.membership.setLatest(peers, 0)
	if !r.VerifyLeader(timeout) {
		return fmt.Errorf("failed transition to leader")
	}
	m := newMembershipChange("", bootstrap)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.membershipChangeCh, m, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(m.errCh, timeoutCh, r.shutdownCh())
}

func (r *Raft) VerifyLeader(timeout time.Duration) bool {
	timeoutCh := getTimeoutCh(timeout)
	select {
	case r.getLeaderState().verifyReqCh <- struct{}{}:
		return true
	case <-timeoutCh:
		return false
	}
}
