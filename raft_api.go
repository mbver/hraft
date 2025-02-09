package hraft

import (
	"fmt"
	"io"
	"sync"
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

func (r *Raft) AddNonVoter(addr string, timeout time.Duration) error {
	m := newMembershipChange(addr, addNonVoter)
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
func (r *Raft) Bootstrap() error { // TODO: timeout is in config?
	hasState, err := r.hasExistingState()
	if err != nil {
		return err
	}
	if hasState {
		return fmt.Errorf("has existing state, can't bootstrap")
	}
	peers := []*Peer{{r.ID(), RoleVoter}}
	r.membership.setLatest(peers, 0)
	checkInterval := (r.config.HeartbeatTimeout + r.config.ElectionTimeout + 10*time.Millisecond) / 4
	success, msg := retry(8, func() (bool, string) {
		<-time.After(checkInterval)
		if r.getStateType() != leaderStateType {
			return false, "failed to transition to leader"
		}
		return true, ""
	})
	if !success {
		return fmt.Errorf("%s", msg)
	}
	m := newMembershipChange("", bootstrap)
	timeoutCh := getTimeoutCh(time.Second)
	if err := sendToRaft(r.membershipChangeCh, m, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(m.errCh, timeoutCh, r.shutdownCh())
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
		return fmt.Errorf("%s error waiting for restore applied: %w", r.ID(), err)
	}
	// force follower to install snapshot.
	// if noOp failed to commit, the leader is unable to enforce consistent state
	// on majority of nodes. it might already loose leadership.
	// the snapshot is not accepted in the cluster.
	noOp := newApply(LogNoOp, nil)
	if err := sendToRaft(r.applyCh, noOp, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	if err := drainErr(noOp.errCh, timeoutCh, r.shutdownCh()); err != nil {
		return fmt.Errorf("%s error waiting for noOp committed after restore: %w", r.ID(), err)
	}
	return nil
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

type verifyLeaderRequest struct {
	l            sync.Mutex
	numRequired  int
	numConfirmed int
	done         bool
	errCh        chan error
}

func (v *verifyLeaderRequest) confirm(isLeader bool, id string) {
	v.l.Lock()
	defer v.l.Unlock()
	if v.done {
		return
	}
	// the only case for negative response
	// is stale term
	if !isLeader {
		v.done = true
		trySend(v.errCh, errNotLeader(id))
		return
	}
	v.numConfirmed++
	if v.numConfirmed >= v.numRequired {
		v.done = true
		trySend(v.errCh, nil)
	}
}

func (v *verifyLeaderRequest) setNumRequired(n int) {
	v.l.Lock()
	defer v.l.Unlock()
	v.numRequired = n
}

func newVerifyLeaderRequest() *verifyLeaderRequest {
	return &verifyLeaderRequest{
		errCh: make(chan error, 1),
	}
}

func (r *Raft) VerifyLeader(timeout time.Duration) error {
	req := newVerifyLeaderRequest()
	timeoutCh := getTimeoutCh(timeout)
	if err := sendToRaft(r.verifyLeaderCh, req, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(req.errCh, timeoutCh, r.shutdownCh())
}

func (r *Raft) LastLeaderContact() time.Time {
	return r.leaderContact.get()
}

// Barrier commits do not mutate application state.
// When it is notified, it means all preceding commits applied.
func (r *Raft) Barrier(timeout time.Duration) error {
	timeoutCh := getTimeoutCh(timeout)
	apply := newApply(LogBarrier, nil)
	if err := sendToRaft(r.applyCh, apply, timeoutCh, r.shutdownCh()); err != nil {
		return err
	}
	return drainErr(apply.errCh, timeoutCh, r.shutdownCh())
}
