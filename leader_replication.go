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
	// extracted from raft config for convenient use
	commitSyncInterval time.Duration
	heartbeatTimeout   time.Duration
	// logAddedCh is notified every time new entries are appended to the log.
	logAddedCh chan struct{}
	// trigger a heartbeat immediately
	pulseCh chan struct{}
	// leader's stepdown control
	stepdown *ResetableProtectedChan
	// stopCh fires when the follower is removed from cluster
	stopCh chan struct{}
	// waiting time to retry when replication fails
	backoff   *backoff
	logSyncCh chan struct{}
	onStage   bool
}

func (r *peerReplication) getNextIdx() uint64 {
	return atomic.LoadUint64(&r.nextIdx)
}

func (r *peerReplication) setNextIdx(idx uint64) {
	atomic.StoreUint64(&r.nextIdx, idx)
}

// wait for signals fires from timeCh or sigCh until the replication is stopped.
// if replication is stopped, return false to signify waiting goro to stop too.
func (r *peerReplication) waitForSignals(timeCh <-chan time.Time, sigCh chan struct{}) (gotSignal bool) {
	select {
	case <-timeCh:
		return true
	case <-sigCh:
		return true
	case <-r.stopCh:
		return false
	case <-r.stepdown.Ch():
		return false
	case <-r.raft.shutdownCh():
		return false
	}
}

func (r *peerReplication) run() {
	// don't run if raft is shutdown
	if !r.raft.wg.Add(1) {
		return
	}
	defer r.raft.wg.Done()
	// Start an async heartbeating routing
	go r.heartbeat()
	for {
		if !r.waitForSignals(jitterTimeoutCh(r.commitSyncInterval), r.logAddedCh) {
			if !tryGetNotify(r.raft.shutdownCh()) {
				r.replicate()
			}
			return
		}
		r.replicate()
	}
}

func (r *peerReplication) replicate() {
	uptoIdx, _ := r.raft.getLastLog()
	<-jitterTimeoutCh(r.heartbeatTimeout / 10)
	var nextIdx uint64
	for {
		nextIdx = r.getNextIdx()
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
			r.stepdown.Close() // stop all replication
			waitCh := r.raft.dispatchTransition(followerStateType, res.Term)
			<-waitCh
			return
		}
		if res.Success {
			r.backoff.reset()
			if len(req.Entries) > 0 {
				lastEntry := req.Entries[len(req.Entries)-1]
				r.setNextIdx(lastEntry.Idx + 1)
				r.updateMatchIdx(r.addr, lastEntry.Idx)
			}
			if r.getNextIdx() > uptoIdx || r.stepdown.IsClosed() {
				break
			}
			continue
		}
		nextIdx = min(nextIdx-1, res.LastLogIdx+1) // ====== seems unnecessary?
		r.setNextIdx(max(nextIdx, 1))              // ===== seems unnecssary?
		r.raft.logger.Warn("appendEntries rejected, sending older logs", "peer", r.addr, "next", r.getNextIdx())
		// if failure is NOT because of log
		// inconsistencies, further delay backoff.
		if !res.PrevLogCheckFailed {
			r.backoff.next()
		}
		if !r.waitForSignals(time.After(r.backoff.getValue()), nil) {
			return
		}
	}
	if r.onStage {
		tryNotify(r.logSyncCh)
		r.onStage = false
	}
}

func (r *peerReplication) heartbeat() {
	r.raft.wg.Add(1)
	defer r.raft.wg.Done()
	backoff := newBackoff(10*time.Millisecond, 41*time.Second)
	req := AppendEntriesRequest{
		Term:   r.currentTerm,
		Leader: []byte(r.raft.ID()),
	}
	var resp AppendEntriesResponse
	for {
		if !r.waitForSignals(time.After(backoff.getValue()), nil) {
			return
		}
		// Wait for the next heartbeat interval or pulse (forced-heartbeat)
		if !r.waitForSignals(jitterTimeoutCh(r.heartbeatTimeout/10), r.pulseCh) {
			return
		}
		if err := r.raft.transport.AppendEntries(r.addr, &req, &resp); err != nil {
			r.raft.logger.Error("failed to heartbeat to", "peer", r.addr, "error", err)
			backoff.next()
			continue
		}
		backoff.reset()
	}
}

// require caller holds lock
func (l *Leader) startReplication() {
	lastIdx := l.raft.instate.getLastIdx() // will negotiate to older value with follower
	for _, addr := range l.raft.PeerAddresses() {
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
		raft:               l.raft,
		commitSyncInterval: l.raft.config.CommitSyncInterval,
		heartbeatTimeout:   l.raft.config.HeartbeatTimeout,
		addr:               addr,
		updateMatchIdx:     l.commit.updateMatchIdx,
		logAddedCh:         make(chan struct{}, 1),
		currentTerm:        l.raft.getTerm(),
		nextIdx:            lastIdx + 1,
		stepdown:           l.stepdown,
		stopCh:             make(chan struct{}),
		backoff:            newBackoff(10*time.Millisecond, 41960*time.Millisecond),
		logSyncCh:          l.staging.logSyncCh,
	}
	if l.staging.getId() == r.addr {
		r.onStage = true
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
