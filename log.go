package hraft

import "time"

type LogType uint8

const (
	LogComand LogType = iota
)

type Log struct {
	Idx          uint64
	Term         uint64
	Type         LogType
	Data         []byte
	DispatchedAt time.Time
}
