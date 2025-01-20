package hraft

import (
	"errors"
	"io"
	"sync/atomic"
)

type CommandsState interface {
	BatchSize() int
	ApplyCommands([]*Commit)
	WriteToSnapshot(*Snapshot) error
	Restore(io.ReadCloser) error
}

type MembershipApplier interface {
	ApplyMembership(*Commit)
}

type AppSnapshotRequest struct {
	term              uint64
	idx               uint64
	writeToSnapshotFn func(*Snapshot) error
	errCh             chan error
}

func newAppSnapshotRequest() *AppSnapshotRequest {
	return &AppSnapshotRequest{
		errCh: make(chan error, 1),
	}
}

type AppStateRestoreReq struct {
	term   uint64
	idx    uint64
	source io.ReadCloser
	errCh  chan error
}

func newAppStateRestoreReq(term, idx uint64, source io.ReadCloser) *AppStateRestoreReq {
	return &AppStateRestoreReq{
		term:   term,
		idx:    idx,
		source: source,
		errCh:  make(chan error, 1),
	}
}

type AppState struct {
	mutateCh        chan []*Commit
	snapshotReqCh   chan *AppSnapshotRequest
	restoreReqCh    chan *AppStateRestoreReq
	lastAppliedIdx  uint64
	lastAppliedTerm uint64
	commandState    CommandsState
	membershipState MembershipApplier
	stop            *ProtectedChan
	doneCh          chan struct{}
}

var ErrEmptyCommandState = errors.New("command state is empty: no log is applied yet")

func NewAppState(command CommandsState, membership MembershipApplier) *AppState {
	return &AppState{
		mutateCh:        make(chan []*Commit, 128),
		snapshotReqCh:   make(chan *AppSnapshotRequest),
		restoreReqCh:    make(chan *AppStateRestoreReq),
		commandState:    command,
		membershipState: membership,
		stop:            newProtectedChan(),
		doneCh:          make(chan struct{}, 1),
	}
}

// TODO: doesn't need to be atomic?
func (a *AppState) setLastApplied(idx, term uint64) {
	atomic.StoreUint64(&a.lastAppliedIdx, idx)
	atomic.StoreUint64(&a.lastAppliedTerm, term)
}

func (a *AppState) getLastApplied() (idx, term uint64) {
	return atomic.LoadUint64(&a.lastAppliedIdx), atomic.LoadUint64(&a.lastAppliedTerm)
}

func (a *AppState) Stop() {
	a.stop.Close()
	<-a.doneCh
}

func (a *AppState) receiveMutations() {
	batchSize := a.commandState.BatchSize()
	for {
		select {
		case commits := <-a.mutateCh:
			batch := make([]*Commit, 0, batchSize)
			for _, c := range commits {
				if c.Log.Type == LogCommand {
					batch = append(batch, c)
					if len(batch) == batchSize {
						a.commandState.ApplyCommands(batch)
						a.setLastApplied(c.Log.Idx, c.Log.Term)
						batch = make([]*Commit, 0, batchSize)
					}
				}
				if c.Log.Type == LogMembership {
					a.membershipState.ApplyMembership(c)
					a.setLastApplied(c.Log.Idx, c.Log.Term)
				}
			}
			if len(batch) > 0 {
				a.commandState.ApplyCommands(batch)
				last := batch[len(batch)-1]
				a.setLastApplied(last.Log.Idx, last.Log.Term)
			}
		case req := <-a.snapshotReqCh:
			req.idx, req.term = a.getLastApplied()
			if req.idx == 0 {
				req.errCh <- ErrEmptyCommandState
				return
			}
			req.writeToSnapshotFn = a.commandState.WriteToSnapshot
			req.errCh <- nil
		case req := <-a.restoreReqCh:
			defer req.source.Close()
			err := a.commandState.Restore(req.source)
			req.errCh <- err
			a.setLastApplied(req.idx, req.term)
		case <-a.stop.ch:
			a.doneCh <- struct{}{}
			return
		}
	}
}
