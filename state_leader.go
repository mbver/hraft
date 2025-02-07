package hraft

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const (
	minCheckInterval = 10 * time.Millisecond
)

type Leader struct {
	term                 uint64
	l                    sync.Mutex
	raft                 *Raft
	active               bool
	commit               *commitControl
	inflight             *inflight
	replicationMap       map[string]*peerReplication
	staging              *staging
	inLeadershipTransfer atomic.Bool
	stepdown             *ResetableProtectedChan
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:           r,
		inflight:       newInflight(),
		replicationMap: map[string]*peerReplication{},
		stepdown:       newResetableProtectedChan(),
	}
	l.stepdown.Close()
	return l
}

func (l *Leader) getTerm() uint64 {
	return atomic.LoadUint64(&l.term)
}

func (l *Leader) setTerm(term uint64) {
	atomic.StoreUint64(&l.term, term)
}

func (l *Leader) StepUp() {
	l.l.Lock()
	defer l.l.Unlock()
	if l.active {
		return
	}
	l.active = true // TODO: may not be needed?
	l.stepdown.Reset()
	l.staging = newStaging()
	if id := l.raft.StagingPeer(); id != "" {
		l.staging.stage(id, l.raft.shutdownCh(), l.stepdown.Ch())
	}
	commitIdx := l.raft.instate.getCommitIdx()
	l.commit = newCommitControl(commitIdx, l.raft.commitNotifyCh, l.raft.Voters())
	l.inflight.Reset()
	l.startReplication()
	go l.selfVerify()
	go l.receiveStaging()
}

func (l *Leader) selfVerify() {
	if !l.raft.wg.Add(1) {
		return
	}
	defer l.raft.wg.Done()
	timeout := l.raft.config.LeaderLeaseTimeout
	timeoutCh := time.After(timeout)
	for {
		select {
		case <-timeoutCh:
			success, maxSinceContact := l.checkFollowerContacts()
			if !success {
				<-l.raft.dispatchTransition(followerStateType, l.getTerm())
				return
			}
			timeout = timeout - maxSinceContact
			if timeout < minCheckInterval {
				timeout = minCheckInterval
			}
			// reset timeoutCh
			timeoutCh = time.After(timeout)
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}

func (l *Leader) checkFollowerContacts() (success bool, maxSinceContact time.Duration) {
	voters := l.raft.Voters()
	timeout := l.raft.config.LeaderLeaseTimeout
	contacted := 0
	l.l.Lock()
	defer l.l.Unlock()
	for _, addr := range voters {
		if addr == l.raft.ID() {
			contacted++
			continue
		}
		repl, ok := l.replicationMap[addr]
		if !ok {
			l.raft.logger.Warn("a voter is not in replication map", "peer", addr)
			continue
		}
		sinceContact := time.Since(repl.lastContact.get())
		if sinceContact < timeout {
			contacted++
			if sinceContact > maxSinceContact {
				// if we fails to contact, maxSinceContact is increased
				// to make selfVerify occurs more often
				maxSinceContact = sinceContact
			}
			continue
		}
		if sinceContact <= 3*timeout {
			l.raft.logger.Warn("failed to contact", "peer", addr, "since", sinceContact)
		} else {
			l.raft.logger.Debug("failed to contact", "peer", addr, "since", sinceContact)
		}
	}
	if contacted > len(voters)/2 {
		return true, maxSinceContact
	}
	return false, maxSinceContact
}

func (l *Leader) Stepdown() {
	l.l.Lock()
	defer l.l.Unlock()
	if !l.active {
		return
	}
	l.active = false
	l.stepdown.Close()
	l.replicationMap = map[string]*peerReplication{}
	// we may stepdown for a lot of reasons
	// stale term and self-verify will not set last leader contact
	// so we do it here. it is not the actual contact in the case of
	// self-verify failed. but still better than the stale contact time
	// before we step up!
	l.raft.leaderContact.setNow()
}

func (l *Leader) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		term := l.getTerm()
		if trans.Term >= term {
			l.Stepdown() // wait for all goros stop?
			l.raft.setTerm(trans.Term)
			l.raft.setStateType(followerStateType)
			logFinishTransition(l.raft.logger, trans, leaderStateType, term)
			l.raft.setLeaderId("")
		}
	}
}

// heartbeatTimeout is blocked in leader state
func (l *Leader) HandleHeartbeatTimeout() {}

func (l *Leader) HandleCommitNotify() {
	commitIdx := l.commit.getCommitIdx()
	l.raft.instate.setCommitIdx(commitIdx)

	first := l.inflight.Front() // this can be nil!
	if first == nil {
		l.raft.handleNewLeaderCommit(commitIdx)
		return
	}
	// handle logs from previous leader
	firstIdx := first.Value.(*Apply).log.Idx
	lastApplied := l.raft.instate.getLastApplied()
	if firstIdx-1 > lastApplied {
		l.raft.handleNewLeaderCommit(min(firstIdx-1, commitIdx))
	}

	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)
	// handle logs after stepping up
	l.inflight.l.Lock()
	var next *list.Element
	for e := first; e != nil; e = next {
		next = e.Next()
		a := e.Value.(*Apply)
		if a.log.Idx > commitIdx {
			break
		}
		// remove it early, else no-op item
		// will block when error sent to it 3rd time.
		l.inflight.list.Remove(e)
		// no-op log is skipped
		if a.log.Type == LogNoOp {
			a.errCh <- nil
			continue
		}
		batch = append(batch, &Commit{a.log, a.errCh})
		if len(batch) == batchSize {
			l.raft.applyCommits(batch)
			batch = make([]*Commit, 0, batchSize)
		}
	}
	l.inflight.l.Unlock()
	if len(batch) > 0 {
		l.raft.applyCommits(batch)
	}
	l.raft.instate.setLastApplied(commitIdx)
	if _, latestIdx := l.raft.membership.getLatest(); latestIdx <= commitIdx {
		l.raft.membership.setCommitted(l.raft.membership.getLatest())
		if l.raft.membership.getPeer(l.raft.ID()) != RoleVoter {
			<-l.raft.dispatchTransition(followerStateType, l.getTerm())
		}
	}
}

func (l *Leader) dispatchApplies(applies []*Apply) {
	now := time.Now()
	term := l.getTerm()
	lastIndex, _ := l.raft.instate.getLastIdxTerm()

	n := len(applies)
	logs := make([]*Log, n)
	l.inflight.l.Lock()
	for idx, a := range applies {
		a.dispatchedAt = now     // DO WE NEED THIS?
		a.log.DispatchedAt = now // CONSIDER SKIPPING?
		lastIndex++
		a.log.Idx = lastIndex
		a.log.Term = term
		logs[idx] = a.log
		l.inflight.list.PushBack(a)
	}
	l.inflight.l.Unlock()
	// Write the log entry locally
	if err := l.raft.logs.StoreLogs(logs); err != nil {
		l.raft.logger.Error("failed to commit logs", "error", err)
		for _, a := range applies {
			trySend(a.errCh, err)
		}
		// transition to follower
		<-l.raft.dispatchTransition(followerStateType, l.getTerm())
		return
	}
	l.commit.updateMatchIdx(l.raft.ID(), lastIndex) // ======= lastIdx is increased by applies.

	// Update the last log since it's on disk now
	l.raft.instate.setLastLog(lastIndex, term)

	// Notify the replicators of the new log
	l.l.Lock()
	for _, repl := range l.replicationMap {
		tryNotify(repl.logAddedCh)
	}
	l.l.Unlock()
}

func (l *Leader) HandleApply(a *Apply) {
	if l.inLeadershipTransfer.Load() {
		l.raft.logger.Debug("ignoring an apply request. leadership is transferring...")
		a.errCh <- ErrLeadershipTransferInProgress
		return
	}
	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Apply, 0, batchSize)
	batch = append(batch, a)
	hasApplies := true
	for len(batch) < batchSize && hasApplies {
		select {
		case a := <-l.raft.applyCh:
			batch = append(batch, a)
		case <-time.After(5 * time.Millisecond): // wait a bit to avoid spurious effect of default
			hasApplies = false
		}
	}
	l.dispatchApplies(batch)
}

func (l *Leader) HandleMembershipChange(change *membershipChange) {
	if l.inLeadershipTransfer.Load() {
		l.raft.logger.Debug("ignoring a membership change request. leadership transferring...")
		change.errCh <- ErrLeadershipTransferInProgress
		return
	}
	if !l.raft.membership.isStable() {
		trySend(change.errCh, ErrMembershipUnstable)
		return
	}
	peers, err := l.raft.membership.newPeersFromChange(change)
	if err != nil {
		l.raft.logger.Error("unable to create new peers from change", "error", err)
		change.errCh <- err
		return
	}
	encoded, err := encode(peers)
	if err != nil {
		l.raft.logger.Error("unable to encode peers", "error", err)
	}
	log := &Log{
		Type: LogMembership,
		Data: encoded,
	}
	if change.changeType == bootstrap {
		l.commit.updateVoters(l.raft.membership.getVoters())
	}
	l.dispatchApplies([]*Apply{{
		log:   log,
		errCh: change.errCh,
	}})
	l.raft.membership.setLatest(peers, log.Idx)
	l.commit.updateVoters(l.raft.Voters())
	switch change.changeType {
	case removePeer:
		l.stopPeerReplication(change.addr)
		return
	case addStaging:
		l.staging.stage(change.addr, l.raft.shutdownCh(), l.stepdown.Ch())
	}
	l.l.Lock()
	l.startReplication()
	l.l.Unlock()
}

func (l *Leader) stepdownOrShutdown() error {
	if l.stepdown.IsClosed() {
		return ErrNotLeader
	}
	if l.raft.shutdown.IsClosed() {
		return ErrRaftShutdown
	}
	return nil
}

var ErrAbortedByRestore = errors.New("abort inflight by restore")

func (l *Leader) HandleRestoreRequest(req *userRestoreRequest) {
	if l.inLeadershipTransfer.Load() {
		l.raft.logger.Debug("ignoring a user restore request. leadership transferring...")
		req.errCh <- ErrLeadershipTransferInProgress
		return
	}
	err := l.restoreSnapshot(req.meta, req.source)
	req.errCh <- err
}

func (l *Leader) restoreSnapshot(meta *SnapshotMeta, source io.ReadCloser) error {
	defer source.Close()
	if !l.raft.membership.isStable() {
		return ErrMembershipUnstable
	}
	// cancel all inflight logs
	l.inflight.l.Lock()
	var next *list.Element
	for e := l.inflight.list.Front(); e != nil; e = next {
		next = e.Next()
		e.Value.(*Apply).errCh <- ErrAbortedByRestore
		l.inflight.list.Remove(e)
	}
	l.inflight.l.Unlock()
	term := l.raft.getTerm()
	lastIdx, _ := l.raft.instate.getLastIdxTerm()
	if meta.Idx > lastIdx {
		lastIdx = meta.Idx
	}
	// make sure we have a hole in log idx
	// to avoid reapplying old logs
	// and name collision with source snapshot
	lastIdx++
	peers, mLatestIdx := l.raft.membership.getLatest()
	snapshot, err := l.raft.snapstore.CreateSnapshot(lastIdx, term, peers, mLatestIdx)
	if err != nil {
		return fmt.Errorf("failed to create snapshot %w", err)
	}
	n, err := io.Copy(snapshot, source)
	if err != nil {
		snapshot.Discard()
		return fmt.Errorf("failed to write snapshot %w", err)
	}
	if n != meta.Size {
		snapshot.Discard()
		return fmt.Errorf("failed to write snapshot, size mismatch: %d != %d", n, meta.Size)
	}
	if err := snapshot.Close(); err != nil {
		return fmt.Errorf("failed to finalize snapshot %w", err)
	}
	l.raft.logger.Info("copied to local snapshot", "bytes", n)

	meta, source, err = l.raft.snapstore.OpenSnapshot(snapshot.Name())
	if err != nil {
		return fmt.Errorf("failed to reopen snapshot %s: %w", snapshot.Name(), err)
	}
	defer source.Close()

	appRestoreReq := newAppStateRestoreRequest(meta.Term, meta.Idx, source)
	select {
	case l.raft.appstate.restoreReqCh <- appRestoreReq:
	case <-l.raft.shutdownCh():
		return ErrRaftShutdown
	}
	if err = <-appRestoreReq.errCh; err != nil {
		return fmt.Errorf("failed to restore snapshot %w", err)
	}
	// all outstanding logs will not be applied
	// even replication succeeds and commit notified
	l.raft.instate.setLastLog(lastIdx, term)
	l.raft.instate.setLastApplied(lastIdx)
	l.raft.instate.setLastSnapshot(lastIdx, term)
	return nil
}

var ErrLeadershipTransferInProgress = errors.New("leadership transfer is in progress")

func (l *Leader) HandleLeadershipTransfer(req *leadershipTransfer) {
	if l.inLeadershipTransfer.Load() {
		l.raft.logger.Debug("ignoring leadership transfer request. leadership transfer is in progress")
		trySend(req.errCh, ErrLeadershipTransferInProgress)
		return
	}
	l.inLeadershipTransfer.Store(true)
	if req.addr == l.raft.ID() {
		trySend(req.errCh, fmt.Errorf("leader %s can not transfer leadership to itself", l.raft.ID()))
		return
	}
	if req.addr != "" {
		if l.raft.membership.getPeer(req.addr) != RoleVoter {
			trySend(req.errCh, fmt.Errorf("peer %s is non-voter", req.addr))
		}
	}
	if req.addr == "" {
		addr := l.mostCurrentFollower()
		if addr == "" {
			trySend(req.errCh, fmt.Errorf("unable to find most current follower. voters are %v", l.raft.Voters()))
			return
		}
		req.addr = addr
	}
	go func() {
		defer l.inLeadershipTransfer.Store(false)
		l.l.Lock()
		repl, ok := l.replicationMap[req.addr]
		defer l.l.Unlock()
		if !ok {
			trySend(req.errCh, fmt.Errorf("peer replication for %s not found", req.addr))
			return
		}
		errCh := make(chan error, 1)
		repl.forceReplicateCh <- errCh
		err := <-repl.waitForErrCh(errCh, req.timeoutCh)
		if err != nil {
			trySend(req.errCh, fmt.Errorf("failed to wait for force-replication response: %w", err))
			return
		}
		lastIdx, _ := l.raft.instate.getLastIdxTerm()
		if lastIdx >= repl.getNextIdx() {
			trySend(req.errCh, fmt.Errorf("unable to force replication to latest lastIdx=%d, nextIdx=%d", lastIdx, repl.getNextIdx()))
			return
		}
		errCh = make(chan error, 1)
		go func() {
			resp := CandidateNowResponse{}
			err = l.raft.transport.CandidateNow(req.addr, &CandidateNowRequest{l.getTerm()}, &resp)
			if err != nil {
				errCh <- err
				return
			}
			if !resp.Success {
				errCh <- fmt.Errorf("%s", resp.Msg)
				return
			}
			errCh <- nil // success
		}()
		select {
		case err = <-errCh:
			req.errCh <- err
		case <-req.timeoutCh:
			req.errCh <- fmt.Errorf("timeout waiting for candidate now response")
		}
	}()
}

func (l *Leader) mostCurrentFollower() string {
	l.l.Lock()
	defer l.l.Unlock()
	var mostCurrentId string
	var nextIdx uint64
	for _, id := range l.raft.Voters() {
		if id == l.raft.ID() {
			continue
		}
		repl, ok := l.replicationMap[id]
		if !ok { // how on earth can this happen??
			continue
		}
		if repl.getNextIdx() > nextIdx {
			nextIdx = repl.getNextIdx()
			mostCurrentId = id
		}
	}
	return mostCurrentId
}

func (l *Leader) HandleVerifyLeader(req *verifyLeaderRequest) {
	l.l.Lock()
	defer l.l.Unlock()
	numRequired := len(l.replicationMap)/2 + 1
	// no followers, only 1 leader
	if numRequired == 1 {
		trySend(req.errCh, nil)
		return
	}
	req.setNumRequired(numRequired)
	for _, repl := range l.replicationMap {
		repl.addVerifyRequest(req)
		// trigger an immediate heartbeat
		repl.pulseCh <- struct{}{}
	}
}
