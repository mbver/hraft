package hraft

import "time"

type Config struct {
	LocalID            string
	InitalPeers        []*Peer
	ElectionTimeout    time.Duration // election timeout is 150-300 ms
	HeartbeatTimeout   time.Duration
	CommitSyncInterval time.Duration
	MaxAppendEntries   int
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	NumTrailingLogs    uint64 // num of logs in logstore after snapshotting. for quick replay.
}

func validateConfig(c *Config) bool {
	return c.ElectionTimeout > 0 &&
		c.HeartbeatTimeout > 0 &&
		c.CommitSyncInterval > 0 &&
		c.MaxAppendEntries > 0
}
