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
	// trigger a heartbeat immediately
	pulseCh chan struct{}
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
		case <-jitterTimeoutCh(r.raft.config.CommitSyncInterval):
			lastLogIdx, _ := r.raft.instate.getLastLog()
			r.replicate(lastLogIdx)
		}
	}
}

func (r *peerReplication) replicate(uptoIdx uint64) {
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
			if !res.PrevLogFailed {
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
		// Wait for the next heartbeat interval or forced notify
		select {
		case <-r.pulseCh:
		case <-jitterTimeoutCh(r.raft.config.HeartbeatTimeout / 10):
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
