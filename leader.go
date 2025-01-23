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

type Leader struct {
	term           uint64
	l              sync.Mutex
	raft           *Raft
	active         bool
	commit         *commitControl
	inflight       *inflight
	replicationMap map[string]*peerReplication
	staging        *staging
	verifyReqCh    chan struct{}
	stepdown       *ResetableProtectedChan
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:           r,
		inflight:       newInflight(),
		replicationMap: map[string]*peerReplication{},
		stepdown:       newResetableProtectedChan(),
		verifyReqCh:    make(chan struct{}),
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
		l.staging.stage(id)
	}
	commitIdx := l.raft.instate.getCommitIdx()
	l.commit = newCommitControl(commitIdx, l.raft.commitNotifyCh)
	l.inflight.Reset()
	l.startReplication()
	go l.receiveStaging()
	go l.receiveVerifyRequests()
}

func (l *Leader) receiveVerifyRequests() {
	for {
		select {
		case <-l.verifyReqCh:
		case <-l.stepdown.Ch():
			return
		case <-l.raft.shutdownCh():
			return
		}
	}
}

func (l *Leader) Stepdown() {
	l.l.Lock()
	defer l.l.Unlock()
	if !l.active {
		return
	}
	l.active = false
	l.stepdown.Close()
	// TODO: wait until all goros stop
	l.replicationMap = map[string]*peerReplication{}
	// TODO: transition to follower
}

func (l *Leader) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		term := l.getTerm()
		if trans.Term >= term {
			l.Stepdown() // wait for all goros stop?
			l.raft.setTerm(trans.Term)
			l.raft.setStateType(followerStateType)
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
		l.inflight.list.Remove(e)
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
	// TODO: check leadership transfer?
	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Apply, 0, batchSize)
	batch = append(batch, a)
	hasApplies := true
	for i := 0; hasApplies && i < batchSize; i++ {
		select {
		case a := <-l.raft.applyCh:
			batch = append(batch, a)
		default:
			hasApplies = false
		}
	}
	l.dispatchApplies(batch)
}

func (l *Leader) HandleMembershipChange(change *membershipChange) {
	// TODO: check for leadership transfer?
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
		l.staging.stage(change.addr)
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

	appRestoreReq := newAppStateRestoreReq(meta.Term, meta.Idx, source)
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
