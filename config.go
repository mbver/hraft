package hraft

import "time"

type Config struct {
	ElectionTimeout  time.Duration
	HeartbeatTimeout time.Duration
	MaxAppendEntries int
}
