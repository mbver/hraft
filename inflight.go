package hraft

import (
	"container/list"
	"errors"
	"sync"
)

var ErrLeaderStepdown = errors.New("leader stepdown")

type inflight struct {
	l    sync.Mutex
	list *list.List
}

func newInflight() *inflight {
	return &inflight{
		list: list.New(),
	}
}

func (f *inflight) Front() *list.Element {
	f.l.Lock()
	defer f.l.Unlock()
	return f.list.Front()
}

func (f *inflight) Reset() {
	f.l.Lock()
	defer f.l.Unlock()
	for e := f.list.Front(); e != nil; e = e.Next() {
		e.Value.(*ApplyRequest).errCh <- ErrLeaderStepdown
	}
	f.list = list.New()
}
