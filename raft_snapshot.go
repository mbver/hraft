package hraft

import "fmt"

func (r *Raft) shouldSnapshot() bool {
	lastSnapIdx, _ := r.instate.getLastSnapshot()
	lastLogIdx, err := r.logs.LastIdx()
	if err != nil {
		r.logger.Error("failed to get last log index", "error", err)
		return false
	}
	return lastLogIdx-lastSnapIdx > r.config.SnapshotThreshold
}

func (r *Raft) compactLogs(snapIdx uint64) error {
	firstIdx, err := r.logs.FirstIdx()
	if err != nil {
		return fmt.Errorf("failed to get first log index: %w", err)
	}
	lastIdx, _ := r.getLastLog()
	numTrailing := r.config.NumTrailingLogs
	if lastIdx-firstIdx <= numTrailing {
		return nil
	}
	// only truncate snapshotted logs
	truncatedIdx := min(snapIdx, lastIdx-numTrailing)
	if truncatedIdx <= firstIdx {
		r.logger.Info("no logs to truncate")
		return nil
	}
	if err := r.logs.DeleteRange(firstIdx, truncatedIdx); err != nil {
		return fmt.Errorf("log compaction failed: %w", err)
	}
	return nil
}

func (r *Raft) takeSnapshot() (string, error) {
	req := newAppSnapshotRequest()
	select {
	case r.appstate.snapshotReqCh <- req:
	case <-r.shutdownCh():
		return "", ErrRaftShutdown
	}
	err := <-req.errCh
	if err != nil {
		return "", err
	}

	peers, mCommitIdx := r.membership.getCommitted()
	snap, err := r.snapstore.CreateSnapshot(req.idx, req.term, peers, mCommitIdx)
	if err != nil {
		return "", fmt.Errorf("failed to create snapshot: %v", err)
	}
	if err = req.writeToSnapshotFn(snap); err != nil {
		snap.Discard()
		return "", fmt.Errorf("failed to write to snapshot file: %w", err)
	}
	if err = snap.Close(); err != nil {
		return "", fmt.Errorf("failed to close snapshot: %v", err)
	}
	// set latest snapshot
	if err = r.compactLogs(0); err != nil {
		return "", err
	}
	r.logger.Info("snapshot complete upto", "index", 0)
	return snap.Name(), nil
}
