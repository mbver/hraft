package hraft

import (
	"container/list"
	"sort"
	"sync"
	"time"
)

type commitControl struct {
	l              sync.Mutex
	commitNotifyCh chan struct{}
	matchIdxs      map[string]uint64
	commitIdx      uint64
	startIdx       uint64
}

func (c *commitControl) getCommitIdx() uint64 {
	c.l.Lock()
	defer c.l.Unlock()
	return c.commitIdx
}

func (c *commitControl) updateMatchIdx(id string, idx uint64) {
	c.l.Lock()
	defer c.l.Unlock()
	prev := c.matchIdxs[id]
	if idx <= prev {
		return
	}
	c.matchIdxs[id] = idx
	c.updateCommitIdx()
}

func (c *commitControl) updateCommitIdx() {
	if len(c.matchIdxs) == 0 {
		return
	}

	matched := make([]uint64, 0, len(c.matchIdxs))
	for _, idx := range c.matchIdxs {
		matched = append(matched, idx)
	}
	sort.Slice(matched, func(i, j int) bool {
		return matched[i] < matched[j]
	})

	commitIdx := matched[(len(matched)-1)/2]

	if commitIdx > c.commitIdx && commitIdx >= c.startIdx { // why care about startIdx?
		c.commitIdx = commitIdx
		tryNotify(c.commitNotifyCh)
	}
}

type peerReplication struct {
	notifyCh chan struct{}
}

type Leader struct {
	l              sync.Mutex
	raft           *Raft
	active         bool
	commit         *commitControl
	inflight       *list.List
	replicationMap map[string]*peerReplication
	stepdown       *ResetableProtectedChan
	staleTermCh    chan struct{}
}

func NewLeader(r *Raft) *Leader {
	l := &Leader{
		raft:     r,
		stepdown: newResetableProtectedChan(),
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
	l.staleTermCh = make(chan struct{}, 1)
	go l.receiveStaleTerm()
	// TODO: start replication
}

func (l *Leader) getStaleTermCh() chan struct{} {
	l.l.Lock()
	defer l.l.Unlock()
	return l.staleTermCh
}

func (l *Leader) receiveStaleTerm() {
	for {
		select {
		case <-l.getStaleTermCh():
			// TRANSITION TO FOLLOWER, NOT STEPDOWN
			return
		case <-l.stepdown.Ch():
			return
		case <-l.raft.ShutdownCh():
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
	l.staleTermCh = nil
	// TODO: transition to follower
}

func (l *Leader) HandleNewCommit() {
	commitIdx := l.commit.getCommitIdx()
	l.raft.setCommitIdx(commitIdx)
	l.processNewCommit(commitIdx)
}

type Apply struct {
	log          *Log
	errCh        chan error
	dispatchedAt time.Time
}

func (l *Leader) processNewCommit(idx uint64) {
	first := l.inflight.Front() // this can be nil!
	if first == nil {
		l.raft.processNewLeaderCommit(idx)
		return
	}
	firstIdx := first.Value.(*Apply).log.Idx
	l.raft.processNewLeaderCommit(min(firstIdx-1, idx))

	batchSize := l.raft.config.MaxAppendEntries
	batch := make([]*Commit, 0, batchSize)

	for e := first; e != nil; e = e.Next() {
		a := e.Value.(*Apply)
		if a.log.Idx > idx {
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

	l.raft.instate.setLastApplied(idx)
}

func (l *Leader) dispatchApplies(applies []*Apply) {
	now := time.Now()
	term := l.raft.instate.getTerm()
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
	for _, repl := range l.replicationMap {
		tryNotify(repl.notifyCh)
	}
}
