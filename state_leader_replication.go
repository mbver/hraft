package hraft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pipelineDrainTimeout = 300 * time.Millisecond
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
	// shouldPreparePipeline is switched on
	// when leader successfully replicate logs to follower
	// and switch off when pipeline is already running
	shouldSwitchToPipeline bool
	// reference to replication pipeline in pipeline mode
	pipeline *replicationPipeline
	// extracted from raft config for convenient use
	commitSyncInterval time.Duration
	heartbeatTimeout   time.Duration
	// logAddedCh is notified every time new entries are appended to the log.
	logAddedCh chan struct{}
	// forceReplicateCh is notified during leadership transfer
	forceReplicateCh chan chan error
	verifyL          sync.Mutex
	verifyRequests   []*verifyLeaderRequest
	// trigger a heartbeat immediately to response for verify leader request
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

func (r *peerReplication) waitForReplicationTrigger() (gotSignal bool, errCh chan error) {
	timeoutCh := jitterTimeoutCh(r.commitSyncInterval)
	select {
	case <-timeoutCh:
		return true, nil
	case <-r.logAddedCh:
		return true, nil
	case errCh = <-r.forceReplicateCh:
		return true, errCh
	case <-r.stopCh:
		return false, nil
	case <-r.stepdown.Ch():
		return false, nil
	case <-r.raft.shutdownCh():
		return false, nil
	}
}

func (r *peerReplication) waitForBackoff(b *backoff) (gotSignal bool) {
	timeoutCh := time.After(b.getValue())
	select {
	case <-timeoutCh:
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
	// heartbeat is run in another thread
	go r.heartbeat()
	for {
		gotSignal, errCh := r.waitForReplicationTrigger()
		if !gotSignal {
			if !tryGetNotify(r.raft.shutdownCh()) {
				r.raft.logger.Info(
					"stop running replication, last replication to",
					"peer", r.addr,
					"stepdown", r.stepdown.IsClosed(),
				)
				r.replicate()
			}
			return
		}
		err := r.replicate()
		trySend(errCh, err)
		if err != nil {
			continue
		}
		if r.shouldSwitchToPipeline {
			r.shouldSwitchToPipeline = false
			if !r.stepdown.IsClosed() && !tryGetNotify(r.stopCh) {
				r.runPipeline()
			}
		}
	}
}

func (r *peerReplication) waitForHearbeatTrigger() (gotSignal bool) {
	timeoutCh := jitterTimeoutCh(r.heartbeatTimeout / 10)
	select {
	case <-timeoutCh:
		return true
	case <-r.pulseCh:
		return true
	case <-r.stopCh:
		return false
	case <-r.stepdown.Ch():
		return false
	case <-r.raft.shutdownCh():
		return false
	}
}

func (r *peerReplication) heartbeat() {
	r.raft.wg.Add(1)
	defer r.raft.wg.Done()
	defer r.verifyAll(false)
	backoff := newBackoff(10*time.Millisecond, 41*time.Second)
	req := AppendEntriesRequest{
		Term:   r.currentTerm,
		Leader: []byte(r.raft.ID()),
	}
	var resp AppendEntriesResponse
	for {
		if !r.waitForBackoff(backoff) {
			return
		}
		// Wait for the next heartbeat interval or pulse (forced-heartbeat)
		if !r.waitForHearbeatTrigger() {
			return
		}
		if err := r.raft.transport.AppendEntries(r.addr, &req, &resp); err != nil {
			r.raft.logger.Error("heartbeat: transport append_entries failed", "peer", r.addr, "error", err)
			backoff.next()
			continue
		}
		backoff.reset()
		// resp.Success == false if and only if our term is behind
		if resp.Term > r.currentTerm {
			<-r.raft.dispatchTransition(followerStateType, r.currentTerm)
			return
		}
		r.verifyAll(true)
	}
}

func (r *peerReplication) replicate() error {
	uptoIdx, _ := r.raft.getLastLog()
	<-jitterTimeoutCh(r.heartbeatTimeout / 10)
	var nextIdx uint64
	for {
		nextIdx = r.getNextIdx()
		entries, err := r.raft.getEntries(nextIdx, uptoIdx)
		if err != nil && err != ErrLogNotFound {
			r.raft.logger.Error("failed to get entries", "from", nextIdx, "to", uptoIdx, "error", err)
			return err
		}
		if err == ErrLogNotFound {
			err = r.sendLatestSnapshot()
			if err != nil {
				r.raft.logger.Error("failed to install latest snapshot", "error", err)
				r.waitForBackoff(r.backoff)
				return err
			}
			continue
		}
		prevIdx, prevTerm, err := r.raft.getPrevLog(nextIdx)
		if err != nil {
			r.raft.logger.Error("failed to get prevlog", "error", err)
			return err
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
			r.raft.logger.Error("replicate: transport append_entries failed", "peer", r.addr, "error", err)
			r.backoff.next()
			r.waitForBackoff(r.backoff)
			return err
		}
		if res.Term > r.currentTerm {
			if !r.stepdown.IsClosed() && r.raft.getTerm() == r.currentTerm {
				r.stepdown.Close() // stop all replication
				<-r.raft.dispatchTransition(followerStateType, res.Term)
			}
			return err
		}
		if res.Success {
			r.backoff.reset()
			if len(entries) > 0 {
				lastEntry := req.Entries[len(entries)-1]
				r.setNextIdx(lastEntry.Idx + 1)
				r.updateMatchIdx(r.addr, lastEntry.Idx)
			}
			if r.getNextIdx() > uptoIdx {
				r.shouldSwitchToPipeline = true
				break
			}
			if r.stepdown.IsClosed() {
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
		if !r.waitForBackoff(r.backoff) {
			return fmt.Errorf("raft shutdown or leader stepdown")
		}
	}
	if r.onStage {
		tryNotify(r.logSyncCh)
		r.onStage = false
	}
	return nil
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
		return fmt.Errorf("failed to open snapshot %w", err)
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
		r.backoff.next()
		return err
	}
	if res.Term > req.Term {
		if !r.stepdown.IsClosed() && r.raft.getTerm() == r.currentTerm {
			r.stepdown.Close() // stop all replication
			<-r.raft.dispatchTransition(followerStateType, res.Term)
		}
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

// chan bool might be an overkill
// it is inefficient to reuse this
// in replicate and pipeline loops
func (r *peerReplication) waitForTimeout(timeout time.Duration) chan bool {
	ch := make(chan bool, 1)
	go func() {
		select {
		case <-time.After(timeout):
			ch <- true
		case <-r.stopCh:
			ch <- false
		case <-r.stepdown.Ch():
			ch <- false
		case <-r.raft.shutdownCh():
			ch <- false
		}
	}()
	return ch
}

func (r *peerReplication) waitForErrCh(errCh chan error, timeoutCh <-chan time.Time) chan error {
	resCh := make(chan error, 1)
	go func() {
		select {
		case err := <-errCh:
			resCh <- err
		case <-timeoutCh:
			resCh <- fmt.Errorf("timeout waiting for errCh")
		case <-r.stopCh:
			resCh <- fmt.Errorf("replication stopped")
		case <-r.stepdown.Ch():
			resCh <- fmt.Errorf("leader stepdown")
		case <-r.raft.shutdownCh():
			resCh <- ErrRaftShutdown
		}
	}()
	return resCh
}

func (r *peerReplication) runPipeline() {
	if !r.raft.wg.Add(1) {
		return
	}
	defer r.raft.wg.Done()

	pipe, err := newReplicationPipeline(r.addr, r.raft.transport)
	if err != nil {
		r.raft.logger.Error("failed to create replication pipeline", "peer", r.addr, "error", err)
		return
	}
	r.pipeline = pipe
	r.raft.logger.Info("enter pipeline replication mode", "peer", r.addr)
	defer r.raft.logger.Info("exit pipeline replication mode", "peer", r.addr)
	go r.receivePipelineResponses()

	stopped := false
	for err == nil && !stopped {
		select {
		case <-r.pipeline.doneCh:
			stopped = true
		case <-r.logAddedCh:
			err = r.pipelineReplicate()
		case errCh := <-r.forceReplicateCh:
			// exit pipeline mode and trigger replicate again
			// to make sure error are handled correctly
			r.forceReplicateCh <- errCh
			r.pipelineReplicate()
			<-r.waitForTimeout(pipelineDrainTimeout) // wait while pipeline responses draining
			stopped = true
		case <-time.After(r.commitSyncInterval):
			err = r.pipelineReplicate()
		case <-r.stopCh:
			err = r.pipelineReplicate()
			stopped = true
		case <-r.stepdown.Ch():
			err = r.pipelineReplicate()
			stopped = true
		case <-r.raft.shutdownCh():
			stopped = true
		}
	}
	if err != nil {
		r.raft.logger.Error("error running pipeline",
			"peer", r.addr,
			"error", err,
			"stepdown", r.stepdown.IsClosed(),
			"stop", tryGetNotify(r.stopCh),
		)
	}
	r.pipeline.Close()
	select {
	case <-r.pipeline.doneCh:
	case <-r.raft.shutdownCh():
	}
}

// check for how to exit the loop and stop receive response loop
func (r *peerReplication) pipelineReplicate() error {
	uptoIdx, _ := r.raft.getLastLog()
	var nextIdx uint64
	for {
		nextIdx = r.getNextIdx()
		entries, err := r.raft.getEntries(nextIdx, uptoIdx)
		if err != nil {
			return err
		}
		prevIdx, prevTerm, err := r.raft.getPrevLog(nextIdx)
		if err != nil {
			return err
		}
		req := &AppendEntriesRequest{
			Term:            r.currentTerm,
			Leader:          []byte(r.raft.ID()),
			PrevLogIdx:      prevIdx,
			PrevLogTerm:     prevTerm,
			Entries:         entries,
			LeaderCommitIdx: r.raft.instate.getCommitIdx(),
		}
		if err := r.pipeline.sendRequest(req); err != nil {
			return err
		}
		if len(entries) > 0 {
			lastEntry := entries[len(entries)-1]
			nextIdx = lastEntry.Idx + 1
			r.setNextIdx(nextIdx)
		}
		if r.getNextIdx() > uptoIdx || r.stepdown.IsClosed() {
			return nil
		}
	}
}

func (r *peerReplication) receivePipelineResponses() {
	if !r.raft.wg.Add(1) {
		return
	}
	defer r.raft.wg.Done()
	defer close(r.pipeline.doneCh)
	var err error
	for {
		select {
		case pending := <-r.pipeline.pendingCh:
			req, resp := pending.req, pending.resp
			err = r.pipeline.readResponse(resp)
			if err != nil {
				r.raft.logger.Error("failed to read pipeline response", "peer", r.addr, "error", err)
				return
			}
			if resp.Term > r.currentTerm {
				r.raft.logger.Info("pipeline response: receiving higher term", "current", r.currentTerm, "received", resp.Term)
				if !r.stepdown.IsClosed() && r.raft.getTerm() == r.currentTerm {
					r.stepdown.Close()
					<-r.raft.dispatchTransition(followerStateType, resp.Term)
				}
				return
			}
			if !resp.Success {
				r.raft.logger.Info("pipeline response: unsuccessful")
				return
			}
			if len(req.Entries) > 0 {
				lastEntry := req.Entries[len(req.Entries)-1]
				r.setNextIdx(lastEntry.Idx + 1)
				r.updateMatchIdx(r.addr, lastEntry.Idx)
			}
		// pipeline's stopCh is notified in the case of
		// prepare/send requests failures or
		// stop replication, leader stepdown or raft shutdown
		case <-r.pipeline.stop.Ch():
			return
		}
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
		forceReplicateCh:   make(chan chan error, 1),
		pulseCh:            make(chan struct{}, 1),
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

func (r *peerReplication) addVerifyRequest(req *verifyLeaderRequest) {
	r.verifyL.Lock()
	defer r.verifyL.Unlock()
	r.verifyRequests = append(r.verifyRequests, req)
}

func (r *peerReplication) verifyAll(isLeader bool) {
	r.verifyL.Lock()
	reqs := r.verifyRequests
	r.verifyRequests = nil
	r.verifyL.Unlock()
	for _, req := range reqs {
		req.confirm(isLeader)
	}
}
