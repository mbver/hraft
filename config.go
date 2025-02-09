package hraft

import (
	"fmt"
	"time"
)

type Config struct {
	LocalID            string
	ElectionTimeout    time.Duration // election timeout is 150-300 ms
	HeartbeatTimeout   time.Duration
	LeaderLeaseTimeout time.Duration
	CommitSyncInterval time.Duration
	MaxAppendEntries   int
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	NumTrailingLogs    uint64 // num of logs in logstore after snapshotting. for quick replay.
}

func validateConfig(c Config) error {
	if c.LocalID == "" {
		return fmt.Errorf("LocalID can not be empty")
	}
	if c.HeartbeatTimeout < 5*time.Millisecond {
		return fmt.Errorf("HeartbeatTimeout is too low")
	}
	if c.ElectionTimeout < 5*time.Millisecond {
		return fmt.Errorf("ElectionTimeout is too low")
	}
	if c.CommitSyncInterval < 5*time.Millisecond {
		return fmt.Errorf("CommitSyncInterval is too low")
	}
	if c.MaxAppendEntries <= 0 {
		return fmt.Errorf("MaxAppendEntries must be positive")
	}
	if c.MaxAppendEntries > 1024 {
		return fmt.Errorf("MaxAppendEntries: %d is too large, max: %d", c.MaxAppendEntries, 1024)
	}
	if c.SnapshotInterval < 5*time.Millisecond {
		return fmt.Errorf("SnapshotInterval is too low")
	}
	if c.LeaderLeaseTimeout < 5*time.Millisecond {
		return fmt.Errorf("LeaderLeaseTimeout is too low")
	}
	if c.LeaderLeaseTimeout > c.HeartbeatTimeout {
		return fmt.Errorf("LeaderLeaseTimeout cannot be larger than heartbeat timeout")
	}
	if c.ElectionTimeout < c.HeartbeatTimeout {
		return fmt.Errorf("ElectionTimeout must be equal or greater than Heartbeat Timeout")
	}
	return nil
}

// all reading to Config will create a new object
// to avoid the overhead of synchronization
func (r *Raft) getConfig() Config {
	return r.config.Load().(Config)
}

func (r *Raft) setConfig(c Config) {
	r.config.Store(c)
}

type ReloadableSubConfig struct {
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration
	NumTrailingLogs   uint64
}

func (c Config) mergeReloadableSubConfig(s ReloadableSubConfig) Config {
	c.SnapshotThreshold = s.SnapshotThreshold
	c.SnapshotInterval = s.SnapshotInterval
	c.NumTrailingLogs = s.NumTrailingLogs
	return c
}

func (c Config) getReloadableSubConfig() ReloadableSubConfig {
	return ReloadableSubConfig{
		SnapshotThreshold: c.SnapshotThreshold,
		SnapshotInterval:  c.SnapshotInterval,
		NumTrailingLogs:   c.NumTrailingLogs,
	}
}
