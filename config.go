package hraft

import "time"

type Config struct {
	LocalID            string
	NoElect            bool // indicates no election occurs in the cluster
	InitalPeers        []*Peer
	ElectionTimeout    time.Duration
	HeartbeatTimeout   time.Duration
	CommitSyncInterval time.Duration
	MaxAppendEntries   int
}

func validateConfig(c *Config) bool {
	return c.ElectionTimeout > 0 &&
		c.HeartbeatTimeout > 0 &&
		c.CommitSyncInterval > 0 &&
		c.MaxAppendEntries > 0
}
