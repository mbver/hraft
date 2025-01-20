package hraft

import (
	"sync"
	"sync/atomic"
)

type idxTerm struct {
	l    sync.Mutex
	idx  uint64
	term uint64
}

// TODO: put lastLog and lastSnapshot in one struct?
type internalState struct {
	term         uint64
	commitIdx    uint64
	lastApplied  uint64
	lastLog      *idxTerm
	lastSnapshot *idxTerm
}

func newInternalState() *internalState {
	in := &internalState{
		lastLog:      &idxTerm{},
		lastSnapshot: &idxTerm{},
	}
	return in
}

func (in *internalState) getTerm() uint64 {
	return atomic.LoadUint64(&in.term)
}

func (in *internalState) setTerm(term uint64) {
	atomic.StoreUint64(&in.term, term)
}

func (in *internalState) getCommitIdx() uint64 {
	return atomic.LoadUint64(&in.commitIdx)
}

func (in *internalState) setCommitIdx(idx uint64) {
	atomic.StoreUint64(&in.commitIdx, idx)
}

func (in *internalState) getLastApplied() uint64 {
	return atomic.LoadUint64(&in.lastApplied)
}

func (in *internalState) setLastApplied(idx uint64) {
	atomic.StoreUint64(&in.lastApplied, idx)
}

func (in *internalState) getLastIdx() uint64 {
	lastLogIdx, _ := in.getLastLog()
	lastSnapIdx, _ := in.getLastSnapshot()
	return max(lastLogIdx, lastSnapIdx) // ??? can snapshotIdx exceeds lastLogIdx?
}

func (in *internalState) getLastLog() (uint64, uint64) {
	in.lastLog.l.Lock()
	defer in.lastLog.l.Unlock()
	return in.lastLog.idx, in.lastLog.term
}

func (in *internalState) setLastLog(idx, term uint64) {
	in.lastLog.l.Lock()
	defer in.lastLog.l.Unlock()
	in.lastLog.idx, in.lastLog.term = idx, term
}

func (in *internalState) getLastSnapshot() (uint64, uint64) {
	in.lastSnapshot.l.Lock()
	defer in.lastSnapshot.l.Unlock()
	return in.lastSnapshot.idx, in.lastSnapshot.term
}

func (in *internalState) setLastSnapshot(idx, term uint64) {
	in.lastSnapshot.l.Lock()
	defer in.lastSnapshot.l.Unlock()
	in.lastSnapshot.idx, in.lastSnapshot.term = idx, term
}
