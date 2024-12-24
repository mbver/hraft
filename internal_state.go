package hraft

import "sync"

type internalState struct {
	l               sync.Mutex
	term            uint64
	commitIdx       uint64
	lastApplied     uint64
	lastLogIdx      uint64
	lastLogTerm     uint64
	lastSnapshotIdx uint64
}

func (in *internalState) getTerm() uint64 {
	in.l.Lock()
	defer in.l.Unlock()
	return in.term
}

func (in *internalState) setTerm(term uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	in.term = term
}

func (in *internalState) getCommitIdx() uint64 {
	in.l.Lock()
	defer in.l.Unlock()
	return in.commitIdx
}

func (in *internalState) setCommitIdx(idx uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	in.commitIdx = idx
}

func (in *internalState) getLastApplied() uint64 {
	in.l.Lock()
	defer in.l.Unlock()
	return in.lastApplied
}

func (in *internalState) setLastApplied(idx uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	in.lastApplied = idx
}

func (in *internalState) getLastIdx() uint64 {
	in.l.Lock()
	defer in.l.Unlock()
	return max(in.lastLogIdx, in.lastSnapshotIdx) // ?????
}

func (in *internalState) getLastLog() (uint64, uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	return in.lastLogIdx, in.lastLogTerm
}

func (in *internalState) setLastLog(idx, term uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	in.lastLogIdx = idx
	in.lastLogTerm = term
}

func (in *internalState) setLastSnapshotIdx(idx uint64) {
	in.l.Lock()
	defer in.l.Unlock()
	in.lastSnapshotIdx = idx
}
