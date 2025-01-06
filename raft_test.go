package hraft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCluster_StartStop(t *testing.T) {
	t.Parallel()
	_, cleanup, err := createTestCluster(3)
	defer cleanup()
	require.Nil(t, err)
	time.Sleep(2 * time.Second)
}
