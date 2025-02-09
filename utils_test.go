// Copyright (c) HashiCorp, Inc.
// Copyright (c) 2025 Phuoc Phi
// SPDX-License-Identifier: MPL-2.0
package hraft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJitterTimeoutch(t *testing.T) {
	start := time.Now()
	timeout := jitterTimeoutCh(time.Millisecond)

	select {
	case <-timeout:
		elapsed := time.Since(start)
		require.Less(t, time.Millisecond, elapsed, "fire early")
	case <-time.After(3 * time.Millisecond):
		t.Fatalf("timeout")
	}
}

func TestJitterTimeoutCh_ZeroInterval(t *testing.T) {
	ch := jitterTimeoutCh(0)
	require.Nil(t, ch)
}

func TestMin(t *testing.T) {
	require.Equal(t, min(1, 1), uint64(1))
	require.Equal(t, min(2, 1), uint64(1))
	require.Equal(t, min(1, 2), uint64(1))
}

func TestMax(t *testing.T) {
	require.Equal(t, max(1, 1), uint64(1))
	require.Equal(t, max(2, 1), uint64(2))
	require.Equal(t, max(1, 2), uint64(2))
}

func TestBackoff(t *testing.T) {
	base, max := 10*time.Millisecond, 2560*time.Millisecond
	b := newBackoff(10*time.Millisecond, 2560*time.Millisecond)
	require.Zero(t, b.getValue())
	b.next()
	require.Equal(t, base, b.getValue())
	b.next()
	require.Equal(t, 2*base, b.getValue())
	for i := 0; i < 7; i++ {
		b.next()
	}
	require.Equal(t, max, b.getValue())
	b.next()
	require.Equal(t, max, b.getValue())
	b.reset()
	require.Zero(t, b.getValue())
}
