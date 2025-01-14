package hraft

import "time"

type LogType uint8

const (
	LogCommand LogType = iota
	LogMembership
	LogNoOp
)

func (t LogType) String() string {
	switch t {
	case LogCommand:
		return "log_command"
	case LogMembership:
		return "log_membership"
	case LogNoOp:
		return "log_no_op"
	}
	return "unknown_log_type"
}

type Log struct {
	Idx          uint64
	Term         uint64
	Type         LogType
	Data         []byte
	DispatchedAt time.Time
}
