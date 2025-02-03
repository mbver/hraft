package hraft

import (
	"fmt"
	"io"
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

func newApply(logType LogType, cmd []byte) *Apply {
	return &Apply{
		log: &Log{
			Type: logType,
			Data: cmd,
		},
		errCh: make(chan error, 1),
	}
}

func (r *Raft) Apply(cmd []byte, timeout time.Duration) chan error {
	a := newApply(LogCommand, cmd)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.applyCh, a, timeoutCh, r.shutdownCh()); err != nil {
		a.errCh <- err
	}
	return a.errCh
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

func (r *Raft) DemoteVoter(addr string, timeout time.Duration) error {
	m := newMembershipChange(addr, demotePeer)
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

// TODO: Only verify if replication succeeds?
func (r *Raft) VerifyLeader(timeout time.Duration) bool {
	timeoutCh := getTimeoutCh(timeout)
	select {
	case r.getLeaderState().verifyReqCh <- struct{}{}:
		return true
	case <-timeoutCh:
		return false
	}
}

type userSnapshotRequest struct {
	openSnapshot func() (*SnapshotMeta, io.ReadCloser, error)
	errCh        chan error
}

func newUserSnapshotRequest() *userSnapshotRequest {
	return &userSnapshotRequest{
		errCh: make(chan error, 1),
	}
}

func (r *Raft) Snapshot(timeout time.Duration) (func() (*SnapshotMeta, io.ReadCloser, error), error) {
	req := newUserSnapshotRequest()
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.snapshotReqCh, req, timeoutCh, r.shutdownCh()); err != nil {
		return nil, err
	}
	err := drainErr(req.errCh, timeoutCh, r.shutdownCh())
	return req.openSnapshot, err
}

type userRestoreRequest struct {
	meta   *SnapshotMeta
	source io.ReadCloser
	errCh  chan error
}

func newUserRestoreRequest(meta *SnapshotMeta, source io.ReadCloser) *userRestoreRequest {
	return &userRestoreRequest{
		meta:   meta,
		source: source,
		errCh:  make(chan error, 1),
	}
}

func (r *Raft) Restore(meta *SnapshotMeta, source io.ReadCloser, timeout time.Duration) error {
	req := newUserRestoreRequest(meta, source)
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.restoreReqCh, req, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	if err := drainErr(req.errCh, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	noOp := newApply(LogNoOp, nil)
	if err := sendToRaft(r.applyCh, noOp, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(noOp.errCh, timeoutCh, r.shutdownCh())
}

type leadershipTransfer struct {
	addr      string
	timeoutCh <-chan time.Time
	errCh     chan error
}

func newLeadershipTransfer(addr string, timeoutCh <-chan time.Time) *leadershipTransfer {
	return &leadershipTransfer{
		addr:      addr,
		timeoutCh: timeoutCh,
		errCh:     make(chan error, 1),
	}
}

func (r *Raft) TransferLeadership(addr string, timeout time.Duration) error {
	timeoutCh := getTimeoutCh(timeout)
	transfer := newLeadershipTransfer(addr, timeoutCh)
	if err := sendToRaft(r.leadershipTransferCh, transfer, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	drainTimeoutCh := timeoutCh
	if timeout > 0 {
		drainTimeoutCh = getTimeoutCh(timeout + 10*time.Millisecond) // allow more time to drain
	}
	if err := drainErr(transfer.errCh, drainTimeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	<-time.After(r.config.ElectionTimeout)
	if r.getStateType() != followerStateType {
		return fmt.Errorf("leader does not stepdown")
	}
	return nil
}
