// Copyright (c) HashiCorp, Inc.
// Copyright (c) 2025 Phuoc Phi
// SPDX-License-Identifier: MPL-2.0
package hraft

import "time"

type LogType uint8

const (
	LogCommand LogType = iota
	LogMembership
	LogNoOp
	LogBarrier
)

func (t LogType) String() string {
	switch t {
	case LogCommand:
		return "log_command"
	case LogMembership:
		return "log_membership"
	case LogNoOp:
		return "log_no_op"
	case LogBarrier:
		return "log_barrier"
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
