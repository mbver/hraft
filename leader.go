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
	// currentTerm is the term of this leader, to be included in AppendEntries
	// requests.
	currentTerm uint64
	// nextIndex is the index of the next log entry to send to the follower,
	nextIdx uint64
	// peer's address
	addr string
	// update matchIdx
	updateMatchIdx func(string, uint64)
	// reference to the leader's Raft
	raft *Raft
	// logAddedCh is notified every time new entries are appended to the log.
	logAddedCh chan struct{}
	// stepDownCh fires when leader steps down
	stepdownCh chan struct{}
	// stopCh fires when the follower is removed from cluster
	stopCh chan struct{}
}

func (l *Leader) startReplication() {
	lastIdx := l.raft.instate.getLastIdx()
	l.l.Lock()
	defer l.l.Unlock()
	for _, p := range l.raft.Peers() {
		if p == l.raft.ID() {
			continue
		}
		l.raft.logger.Info("added peer, starting replication", "peer", p)
		r := &peerReplication{
			raft:           l.raft,
			addr:           p,
			updateMatchIdx: l.commit.updateMatchIdx,
			logAddedCh:     make(chan struct{}, 1),
			currentTerm:    l.raft.instate.getTerm(),
			nextIdx:        lastIdx + 1,
			stepdownCh:     l.stepdown.Ch(),
		}

		l.replicationMap[p] = r
		go r.run()
		tryNotify(r.logAddedCh)
	}
}

func (l *Leader) stopPeerReplication(addr string) {
	l.l.Lock()
	defer l.l.Unlock()
	r, ok := l.replicationMap[addr]
	if !ok {
		return
	}
	close(r.stopCh)
	delete(l.replicationMap, addr)
}

func (r *peerReplication) run() {
	// Start an async heartbeating routing
	stopHeartbeatCh := make(chan struct{})
	defer close(stopHeartbeatCh)
	go r.heartbeat(stopHeartbeatCh)
	shouldStop := false
	for !shouldStop {
		select {
		case <-r.stopCh:
			lastLogIdx, _ := r.raft.instate.getLastLog()
			r.replicateTo(lastLogIdx)
			return
		case <-r.stepdownCh:
			return
		case <-r.logAddedCh:
			lastLogIdx, _ := r.raft.instate.getLastLog()
			shouldStop = r.replicateTo(lastLogIdx)
		case <-time.After(jitter(r.raft.config.CommitSyncInterval)):
			lastLogIdx, _ := r.raft.instate.getLastLog()
			shouldStop = r.replicateTo(lastLogIdx)
		}
	}
}

func (r *peerReplication) replicateTo(lastIdx uint64) (shouldStop bool) {
	return false
}

func (r *peerReplication) heartbeat(stopCh chan struct{}) {}

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
	l.replicationMap = map[string]*peerReplication{}
	// TODO: transition to follower
}

func (l *Leader) HandleTransition(trans *Transition) {

}

func (l *Leader) HandleHeartbeatTimeout() {}

func (l *Leader) HandleRPC(rpc *RPC) {}

func (l *Leader) HandleNewCommit() {
	commitIdx := l.commit.getCommitIdx()
	l.raft.instate.setCommitIdx(commitIdx)
	l.processNewCommit(commitIdx)
}

func (l *Leader) HandleApply(a *Apply) {}

func (l *Leader) HandleCommitNotify() {}

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
	l.l.Lock()
	for _, repl := range l.replicationMap {
		tryNotify(repl.logAddedCh)
	}
	l.l.Unlock()

}
