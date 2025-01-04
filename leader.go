package hraft

import (
	"container/list"
	"sync"
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
	stepdown       *ResetableProtectedChan
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:           r,
		replicationMap: map[string]*peerReplication{},
		stepdown:       newResetableProtectedChan(),
	}
	l.stepdown.Close()
	return l
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
	l.startReplication()
	go l.receiveStaging()
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
	// TODO: transition to follower
}

func (l *Leader) HandleTransition(trans *Transition) {
	switch trans.To {
	case followerStateType:
		if trans.Term > l.term {
			l.Stepdown() // wait for all goros stop?
			l.raft.setTerm(trans.Term)
			l.raft.setStateType(followerStateType)
		}
		if trans.Term == l.term {
			panic("two leaders of the same term!")
		}
	}
}

// heartbeatTimeout is blocked in leader state
func (l *Leader) HandleHeartbeatTimeout() {}

func (l *Leader) HandleRPC(rpc *RPC) {}

func (l *Leader) HandleNewCommit() {
	commitIdx := l.commit.getCommitIdx()
	l.raft.instate.setCommitIdx(commitIdx)

	first := l.inflight.Front() // this can be nil!
	if first == nil {
		l.raft.handleNewLeaderCommit(commitIdx)
		return
	}
	firstIdx := first.Value.(*Apply).log.Idx
	l.raft.handleNewLeaderCommit(min(firstIdx-1, commitIdx))

	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)

	for e := first; e != nil; e = e.Next() {
		a := e.Value.(*Apply)
		if a.log.Idx > commitIdx {
			break
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

	l.raft.instate.setLastApplied(commitIdx)
}

func (l *Leader) HandleApply(a *Apply) {}

func (l *Leader) HandleCommitNotify() {}

type Apply struct {
	log          *Log
	errCh        chan error
	dispatchedAt time.Time
}

func (l *Leader) dispatchApplies(applies []*Apply) {
	now := time.Now()
	term := l.term
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
			trySendErr(a.errCh, err)
		}
		// TRANSITION TO FOLLOWER
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

func (l *Leader) HandleMembershipChange(change *membershipChange) { // return errors?
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
		Type: LogMember,
		Data: encoded,
	}
	l.dispatchApplies([]*Apply{{
		log: log,
	}})
	l.raft.membership.setLatest(peers, log.Idx)
	l.commit.updateVoters(l.raft.Voters())
	l.startReplication()
	switch change.changeType {
	case addStaging:
		l.staging.stage(change.addr)
	case promotePeer:
		l.staging.promote(change.addr)
	case demotePeer:
		// TODO
	case removePeer:
		// TODO
	}
}
