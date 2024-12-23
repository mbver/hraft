package hraft

import "sync"

type shutdown struct {
	done bool
	ch   chan struct{}
	l    sync.Mutex
}

func (s *shutdown) Shutdown() {
	s.l.Lock()
	defer s.l.Unlock()
	if s.done {
		return
	}
	s.done = true
	close(s.ch)
}

func (s *shutdown) Done() bool {
	s.l.Lock()
	defer s.l.Unlock()
	return s.done
}

func (s *shutdown) Ch() chan struct{} {
	return s.ch
}

func newShutdown() *shutdown {
	return &shutdown{
		ch: make(chan struct{}),
	}
}
