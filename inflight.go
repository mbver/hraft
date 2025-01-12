package hraft

import (
	"container/list"
	"sync"
)

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

func (f *inflight) Pushback(a *Apply) *list.Element {
	f.l.Lock()
	defer f.l.Unlock()
	return f.list.PushBack(a)
}

func (f *inflight) Remove(e *list.Element) any {
	f.l.Lock()
	defer f.l.Unlock()
	return f.list.Remove(e)
}

func (f *inflight) Reset() {
	f.l.Lock()
	defer f.l.Unlock()
	f.list = list.New()
}
