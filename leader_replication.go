package hraft

import (
	"sync/atomic"
	"time"
)

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
	// notify leader that the peers'log is synced
	logSyncedCh chan struct{}
	// trigger a heartbeat immediately
	pulseCh chan struct{}
	// leader's stepdown control
	stepdown *ResetableProtectedChan
	// stopCh fires when the follower is removed from cluster
	stopCh chan struct{}
	// waiting time to retry when replication fails
	backoff *backoff
	staging *staging
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
			r.replicate()
			return
		case <-r.stepdown.Ch():
			return
		case <-r.raft.shutdownCh():
			return
		case <-r.logAddedCh:
			r.replicate()
		case <-jitterTimeoutCh(r.raft.config.CommitSyncInterval):
			r.replicate()
		}
	}
}

func (r *peerReplication) replicate() {
	uptoIdx, _ := r.raft.getLastLog()
	<-jitterTimeoutCh(r.raft.config.HeartbeatTimeout / 10)
	nextIdx := r.getNextIdx()
	for nextIdx < uptoIdx && !r.stepdown.IsClosed() {
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
			Term:            r.currentTerm,
			Leader:          []byte(r.raft.ID()),
			PrevLogIdx:      prevIdx,
			PrevLogTerm:     prevTerm,
			Entries:         entries,
			LeaderCommitIdx: r.raft.instate.getCommitIdx(),
		}
		res := &AppendEntriesResponse{}
		if err = r.raft.transport.AppendEntries(r.addr, req, res); err != nil {
			r.backoff.next()
			return
		}
		if res.Term > r.currentTerm {
			r.stepdown.Close() // stop all replication early
			waitCh := r.raft.dispatchTransition(followerStateType, res.Term)
			<-waitCh
			return
		}
		if !res.Success {
			nextIdx = min(nextIdx-1, res.LastLogIdx+1) // ====== seems unnecessary?
			r.setNextIdx(max(nextIdx, 1))              // ===== seems unnecssary?
			r.raft.logger.Warn("appendEntries rejected, sending older logs", "peer", r.addr, "next", r.getNextIdx())
			// if replicate failed not because of log-consistency check,
			// delay retry futher
			if !res.PrevLogCheckFailed {
				r.backoff.next()
			}
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
	if r.stepdown.IsClosed() || r.staging == nil {
		return
	}
	if !r.staging.isActive() || r.staging.getId() != r.addr {
		return
	}
	r.staging.logSyncCh <- r.addr
}

func (r *peerReplication) heartbeat(stopCh chan struct{}) {
	backoff := newBackoff(10*time.Millisecond, 41*time.Second)
	req := AppendEntriesRequest{
		Term:   r.currentTerm,
		Leader: []byte(r.raft.ID()),
	}
	var resp AppendEntriesResponse
	for {
		select {
		case <-time.After(backoff.getValue()):
		case <-stopCh:
		}
		// Wait for the next heartbeat interval or pulse (forced-heartbeat)
		select {
		case <-time.After(r.raft.config.HeartbeatTimeout):
			<-jitterTimeoutCh(r.raft.config.HeartbeatTimeout / 10)
		case <-r.pulseCh:
		case <-stopCh:
			return
		}

		if err := r.raft.transport.AppendEntries(r.addr, &req, &resp); err != nil {
			r.raft.logger.Error("failed to heartbeat to", "peer", r.addr, "error", err)
			backoff.next()
			continue
		}
		backoff.reset()
		// TODO: verify if we are still leader and notify those waiting for leadership-check
	}
}

// require caller holds lock
func (l *Leader) startReplication() {
	lastIdx := l.raft.instate.getLastIdx() // will negotiate to older value with follower
	for _, addr := range l.raft.Peers() {
		if addr == l.raft.ID() {
			continue
		}
		_, ok := l.replicationMap[addr]
		if ok {
			continue
		}
		l.raft.logger.Info("added peer, starting replication", "peer", addr)
		r := l.startPeerReplication(addr, lastIdx)
		l.replicationMap[addr] = r
	}
}

func (l *Leader) startPeerReplication(addr string, lastIdx uint64) *peerReplication {
	r := &peerReplication{
		raft:           l.raft,
		addr:           addr,
		updateMatchIdx: l.commit.updateMatchIdx,
		logAddedCh:     make(chan struct{}, 1),
		currentTerm:    l.raft.getTerm(),
		nextIdx:        lastIdx + 1,
		stepdown:       l.stepdown,
	}
	go r.run()
	tryNotify(r.logAddedCh)
	return r
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
