package hraft

import "time"

type Config struct {
	ElectionTimeout time.Duration
}
