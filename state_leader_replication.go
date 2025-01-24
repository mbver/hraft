package hraft

import (
	"fmt"
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
		entries, err := r.raft.getEntries(nextIdx, uptoIdx)
		if err != nil && err != ErrLogNotFound {
			r.raft.logger.Error("failed to get entries", "from", nextIdx, "to", uptoIdx, "error", err)
			return
		}
		if err == ErrLogNotFound {
			err = r.sendLatestSnapshot()
			if err != nil {
				r.raft.logger.Error("failed to install latest snapshot", "error", err)
				r.backoff.next()
				r.waitForSignals(time.After(r.backoff.getValue()), nil)
				return
			}
			continue
		}
		prevIdx, prevTerm, err := r.raft.getPrevLog(nextIdx)
		if err != nil {
			r.raft.logger.Error("failed to get prevlog", "error", err)
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
			<-r.raft.dispatchTransition(followerStateType, res.Term)
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
		nextIdx = min(nextIdx-1, res.LastLogIdx+1)
		r.setNextIdx(max(nextIdx, 1))
		r.raft.logger.Warn("appendEntries rejected, sending older logs", "peer", r.addr, "next", r.getNextIdx())
		// if failure is NOT because of inconsistencies
		// further delay backoff.
		if !res.PrevLogCheckFailed {
			r.backoff.next()
		}
		// in the case leader is removed
		// we expect last replication will update leader-commit
		// and applied new membership to followers
		// so this wait has to be put at the end of loop
		if !r.waitForSignals(time.After(r.backoff.getValue()), nil) {
			return
		}
	}
	if r.onStage {
		tryNotify(r.logSyncCh)
		r.onStage = false
	}
}

func (r *peerReplication) sendLatestSnapshot() error {
	metas, err := r.raft.snapstore.List()
	if err != nil {
		r.raft.logger.Error("failed to list snapshots", "error", err)
		return err
	}
	if len(metas) == 0 {
		r.raft.logger.Error("no snapshot in store")
		return fmt.Errorf("no snapshots found")
	}
	meta, snapshot, err := r.raft.snapstore.OpenSnapshot(metas[0].Name)
	if err != nil {
		r.raft.logger.Error("failed to open snapshot", "name", metas[0].Name, "error", err)
		return err
	}
	defer snapshot.Close()

	req := &InstallSnapshotRequest{
		Term:        r.currentTerm,
		LastLogIdx:  meta.Idx,
		LastLogTerm: meta.Term,
		Size:        meta.Size,
		Peers:       meta.Peers,
		MCommitIdx:  meta.MCommitIdx,
	}
	res := &InstallSnapshotResponse{}
	if err := r.raft.transport.InstallSnapshot(r.addr, req, res, snapshot); err != nil {
		return err
	}
	if res.Term > req.Term {
		r.stepdown.Close() // stop all replication
		<-r.raft.dispatchTransition(followerStateType, res.Term)
		return fmt.Errorf("stale term: current: %d, received: %d", r.currentTerm, res.Term)
	}
	if res.Success {
		r.setNextIdx(meta.Idx + 1)
		r.updateMatchIdx(r.addr, meta.Idx)
		r.backoff.reset()
		// NOTIFY?
		return nil
	}
	r.backoff.next()
	r.raft.logger.Warn("installSnapshot rejected", "peer", r.addr)
	return fmt.Errorf("installSnapshot rejected peer=%s", r.addr)
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
	lastIdx, _ := l.raft.instate.getLastIdxTerm() // will negotiate to older value with follower
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
