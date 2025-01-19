package hraft

import (
	"sync/atomic"
)

type IdxTerm struct {
	idx  uint64
	term uint64
}

// TODO: replace lock with atomic for most of fields
type internalState struct {
	term         uint64
	commitIdx    uint64
	lastApplied  uint64
	lastLog      atomic.Value
	lastSnapshot atomic.Value
}

func newInternalState() *internalState {
	in := &internalState{}
	in.setLastLog(0, 0)
	in.setLastSnapshot(0, 0)
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
	lastLog := in.lastLog.Load().(*IdxTerm)
	return lastLog.idx, lastLog.term
}

func (in *internalState) setLastLog(idx, term uint64) {
	in.lastLog.Store(&IdxTerm{idx, term})
}

func (in *internalState) getLastSnapshot() (uint64, uint64) {
	lastSnapshot := in.lastSnapshot.Load().(*IdxTerm)
	return lastSnapshot.idx, lastSnapshot.term
}

func (in *internalState) setLastSnapshot(idx, term uint64) {
	in.lastSnapshot.Store(&IdxTerm{idx, term})
}
