package hraft

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotStore_CreateSnapshot_NonExistDir(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	require.Equal(t, uint64(2), meta.MCommitIdx)
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
	t.Parallel()
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

func TestSnapshotStore_Retain(t *testing.T) {
	t.Parallel()
	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	store, err := NewSnapshotStore(baseDir, 2, nil)
	require.Nil(t, err)
	require.Nil(t, checkNumSnapshotsInStore(store, 0))

	for i := 0; i < 5; i++ {
		snap, err := store.CreateSnapshot(uint64(10+i), 3, []*Peer{}, 1)
		require.Nil(t, err)
		err = snap.Close()
		require.Nil(t, err)
		if i == 0 {
			require.Nil(t, checkNumSnapshotsInStore(store, 1))
			continue
		}
		require.Nil(t, checkNumSnapshotsInStore(store, 2))
	}

	metas, _ := store.List()
	require.Equal(t, uint64(14), metas[0].Idx)
	require.Equal(t, uint64(13), metas[1].Idx)
}

func TestSnapshotStore_BadPermission(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("skipping file permission test on windows")
	}

	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	err = os.Chmod(baseDir, 000)
	require.Nil(t, err)
	defer os.Chmod(baseDir, 0777)

	_, err = NewSnapshotStore(baseDir, 2, nil)
	require.NotNil(t, err)
}

func TestSnapshotStore_Ordering(t *testing.T) {
	t.Parallel()
	baseDir, err := os.MkdirTemp("", "raft")
	require.Nil(t, err)
	defer os.RemoveAll(baseDir)

	store, err := NewSnapshotStore(baseDir, 2, nil)
	require.Nil(t, err)

	snap, err := store.CreateSnapshot(130350, 5, []*Peer{}, 1)
	require.Nil(t, err)
	err = snap.Close()
	require.Nil(t, err)

	snap, err = store.CreateSnapshot(230350, 36, []*Peer{}, 1)
	require.Nil(t, err)
	err = snap.Close()
	require.Nil(t, err)

	metas, err := store.List()
	require.Nil(t, err)
	require.Equal(t, 2, len(metas))
	require.Equal(t, uint64(36), metas[0].Term)
	require.Equal(t, uint64(5), metas[1].Term)
}
