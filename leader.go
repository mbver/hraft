package hraft

import (
	"container/list"
	"sort"
	"sync"
	"sync/atomic"
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
	// leader's stepdown control
	stepdown *ResetableProtectedChan
	// stopCh fires when the follower is removed from cluster
	stopCh chan struct{}
	// waiting time to retry when replication fails
	backoff *backoff
}

func (r *peerReplication) getNextIdx() uint64 {
	return atomic.LoadUint64(&r.nextIdx)
}

func (r *peerReplication) setNextIdx(idx uint64) {
	atomic.StoreUint64(&r.nextIdx, idx)
}

func (r *peerReplication) run() {
	// Start an async heartbeating routing
	stopHeartbeatCh := make(chan struct{})
	defer close(stopHeartbeatCh)
	go r.heartbeat(stopHeartbeatCh)
	for {
		select {
		case <-r.stopCh:
			lastLogIdx, _ := r.raft.instate.getLastLog()
			r.replicate(lastLogIdx)
			return
		case <-r.stepdown.Ch():
			return
		case <-r.logAddedCh:
			lastLogIdx, _ := r.raft.instate.getLastLog()
			r.replicate(lastLogIdx)
		case <-time.After(jitter(r.raft.config.CommitSyncInterval)):
			lastLogIdx, _ := r.raft.instate.getLastLog()
			r.replicate(lastLogIdx)
		}
	}
}

func (r *peerReplication) replicate(uptoIdx uint64) {
	nextIdx := r.getNextIdx()

	for nextIdx <= uptoIdx && r.stepdown.IsClosed() {
		select {
		case <-time.After(r.backoff.getValue()):
		case <-r.stepdown.Ch():
			return
		}

		prevIdx, prevTerm, err := r.raft.getPrevLog(nextIdx)
		// skip snapshot stuffs for now
		if err != nil { // reporting error and stop node ??
			return
		}
		entries, err := r.raft.getEntries(nextIdx, uptoIdx)
		if err != nil {
			return
		}
		req := &AppendEntriesRequest{
			Term:         r.currentTerm,
			Leader:       []byte(r.raft.ID()),
			PrevLogIdx:   prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: r.raft.instate.getCommitIdx(),
		}
		res := &AppendEntriesResponse{}
		if err = r.raft.transport.AppendEntries(r.addr, req, res); err != nil {
			r.backoff.next()
			return
		}
		if res.Term > r.currentTerm { // send to staleTermCh or stepdownCh?
			r.stepdown.Close() // stop all replication early
			r.raft.transitionCh <- &Transition{followerStateType, res.Term}
			return
		}
		if !res.Success {
			r.backoff.next()
			nextIdx = min(nextIdx-1, res.LastLogIdx+1) // ====== seems unnecessary?
			r.setNextIdx(max(nextIdx, 1))              // ===== seems unnecssary?
			r.raft.logger.Warn("appendEntries rejected, sending older logs", "peer", r.addr, "next", r.getNextIdx())
			continue
		}
		r.backoff.reset()
		if len(req.Entries) > 0 {
			lastEntry := req.Entries[len(req.Entries)-1]
			r.setNextIdx(lastEntry.Idx + 1)
			r.updateMatchIdx(r.addr, lastEntry.Idx)
		}
		nextIdx = r.getNextIdx()
	}
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
	// TODO: start replication
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
			stepdown:       l.stepdown,
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
