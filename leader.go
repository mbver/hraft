package hraft

import (
	"container/list"
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
	inflight       *list.List
	replicationMap map[string]*peerReplication
	staging        *staging
	verifyReqCh    chan struct{}
	stepdown       *ResetableProtectedChan
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:           r,
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
	l.inflight = list.New()
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
	l.inflight = nil
	// TODO: transition to follower
}

func (l *Leader) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		term := l.getTerm()
		if trans.Term > term {
			l.Stepdown() // wait for all goros stop?
			l.raft.setTerm(trans.Term)
			l.raft.setStateType(followerStateType)
		}
		if trans.Term == term {
			panic("two leaders of the same term!")
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
	firstIdx := first.Value.(*Apply).log.Idx
	// handle logs from previous leader
	l.raft.handleNewLeaderCommit(min(firstIdx-1, commitIdx))

	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)
	// handle logs after stepping up
	for e := first; e != nil; e = e.Next() {
		a := e.Value.(*Apply)
		if a.log.Idx > commitIdx {
			break
		}
		// no-op log is skipped
		if a.log.Type == LogNoOp {
			continue
		}
		batch = append(batch, &Commit{a.log, a.errCh})
		l.inflight.Remove(e)
		if len(batch) == batchSize {
			l.raft.applyCommits(batch)
			batch = make([]*Commit, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		l.raft.applyCommits(batch)
	}
	if _, latestIdx := l.raft.membership.getLatest(); latestIdx <= commitIdx {
		l.raft.membership.setCommitted(l.raft.membership.getLatest())
	}
	l.raft.instate.setLastApplied(commitIdx)
}

func (l *Leader) HandleApply(a *Apply) {
	// get maxAppendentries items
	// dispatch applies
}

func (l *Leader) dispatchApplies(applies []*Apply) {
	now := time.Now()
	term := l.getTerm()
	lastIndex := l.raft.instate.getLastIdx() // ???

	n := len(applies)
	logs := make([]*Log, n)

	for idx, a := range applies {
		a.dispatchedAt = now     // DO WE NEED THIS?
		a.log.DispatchedAt = now // CONSIDER SKIPPING?
		lastIndex++
		a.log.Idx = lastIndex
		a.log.Term = term
		logs[idx] = a.log
		l.inflight.PushBack(a)
	}
	// Write the log entry locally
	if err := l.raft.logs.StoreLogs(logs); err != nil {
		l.raft.logger.Error("failed to commit logs", "error", err)
		for _, a := range applies {
			trySend(a.errCh, err)
		}
		// transition to follower
		waitCh := l.raft.dispatchTransition(followerStateType, l.getTerm())
		<-waitCh
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

func (l *Leader) HandleMembershipChange(change *membershipChange) {
	if !l.raft.membership.isStable() {
		trySend(change.errCh, ErrMembershipUnstable)
		return
	}
	peers, err := l.raft.membership.newPeersFromChange(change)
	if err != nil {
		l.raft.logger.Error("unable to create new peers from change", "error", err)
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
	l.l.Lock()
	l.startReplication()
	l.l.Unlock()
	switch change.changeType {
	case addStaging:
		l.staging.stage(change.addr)
	case promotePeer:
		// DO NOTHING?
	case demotePeer:
		// TODO
	case removePeer:
		// TODO
	}
}
