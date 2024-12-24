package hraft

import "time"

type Config struct {
	ElectionTimeout  time.Duration
	MaxAppendEntries int
}
