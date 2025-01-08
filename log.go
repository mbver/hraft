package hraft

import "time"

type LogType uint8

const (
	LogCommand LogType = iota
	LogMembership
	LogNoOp
)

type Log struct {
	Idx          uint64
	Term         uint64
	Type         LogType
	Data         []byte
	DispatchedAt time.Time
}
