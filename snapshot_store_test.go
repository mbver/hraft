package hraft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotStore_CreateSnapshot_NonExistDir(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	os.RemoveAll(baseDir)
	store, err := NewSnapshotStore(baseDir, 3, nil)
	require.Nil(t, err)

	os.Remove(baseDir)
	_, err = store.CreateSnapshot(10, 3, []*Peer{}, 1)
	require.Nil(t, err)
}

func checkNumSnapshotsInStore(store *SnapshotStore, num int) error {
	metas, err := store.List()
	if err != nil {
		return err
	}
	if len(metas) != num {
		return fmt.Errorf("mismatch number of snaphshots, expect: %d, got: %d", num, len(metas))
	}
	return nil
}

func TestSnapshotStore_CreateSnapshot(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	store, err := NewSnapshotStore(baseDir, 3, nil)
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))

	peers := []*Peer{&Peer{
		ID:   "127.0.0.1:4567",
		Role: RoleVoter,
	}}
	snap, err := store.CreateSnapshot(10, 3, peers, 2)
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))

	_, err = snap.Write([]byte("first\n"))
	require.Nil(t, err)
	_, err = snap.Write([]byte("second\n"))
	require.Nil(t, err)

	err = snap.Close()
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 1))

	metas, _ := store.List()
	meta := metas[0]
	require.Equal(t, uint64(10), meta.Idx)
	require.Equal(t, uint64(3), meta.Term)
	require.True(t, reflect.DeepEqual(peers, meta.Peers))
	require.Equal(t, uint64(2), meta.MembershipCommittedIdx)
	require.Equal(t, int64(13), meta.Size)

	_, bufFile, err := store.OpenSnapshot(meta.Name)
	require.Nil(t, err)

	buf := bytes.Buffer{}
	_, err = io.Copy(&buf, bufFile)
	require.Nil(t, err)

	err = bufFile.Close()
	require.Nil(t, err)

	require.True(t, bytes.Equal(buf.Bytes(), []byte("first\nsecond\n")))
}

func TestSnapshot_Discard(t *testing.T) {
	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	store, err := NewSnapshotStore(baseDir, 3, nil)
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))

	snap, err := store.CreateSnapshot(10, 3, nil, 2)
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))

	err = snap.Discard()
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))
}
