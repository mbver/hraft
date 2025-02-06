package hraft

import (
	"fmt"
	"sync"
)

// LogCache wraps any LogStore implementation to provide an
// in-memory ring buffer. This is used to cache access to
// the recently written entries. For implementations that do not
// cache themselves, this can provide a substantial boost by
// avoiding disk I/O on recent entries.
type LogCache struct {
	store LogStore
	l     sync.RWMutex
	cache []*Log
}

func NewLogCache(capacity int, store LogStore) (*LogCache, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity must be positive")
	}
	c := &LogCache{
		store: store,
		cache: make([]*Log, capacity),
	}
	return c, nil
}

func (c *LogCache) GetLog(idx uint64, log *Log) error {
	c.l.RLock()
	cached := c.cache[idx%uint64(len(c.cache))]
	c.l.RUnlock()

	// check if entry is valid
	if cached != nil && cached.Idx == idx {
		*log = *cached
		return nil
	}
	// forward request on cache miss
	return c.store.GetLog(idx, log)
}

func (c *LogCache) StoreLog(log *Log) error {
	return c.StoreLogs([]*Log{log})
}

func (c *LogCache) StoreLogs(logs []*Log) error {
	err := c.store.StoreLogs(logs)
	// Insert the logs into the ring buffer, but only on success
	if err != nil {
		return fmt.Errorf("unable to store logs within log store, err: %q", err)
	}
	c.l.Lock()
	for _, l := range logs {
		c.cache[l.Idx%uint64(len(c.cache))] = l
	}
	c.l.Unlock()
	return nil
}

func (c *LogCache) FirstIdx() (uint64, error) {
	return c.store.FirstIdx()
}

func (c *LogCache) LastIdx() (uint64, error) {
	return c.store.LastIdx()
}

func (c *LogCache) DeleteRange(min, max uint64) error {
	// Invalidate the cache on deletes
	c.l.Lock()
	c.cache = make([]*Log, len(c.cache))
	c.l.Unlock()

	return c.store.DeleteRange(min, max)
}
