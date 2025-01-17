package hraft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	metafileName = "meta.json"
	tmpSuffix    = ".tmp"
)

type Snapshot struct {
	dir     string
	logFile string
	buf     *bufio.Writer
	logger  hclog.Logger
	closed  bool
}

type SnapshotMeta struct {
	Path                   string
	Term                   uint64
	Idx                    uint64
	Peers                  []*Peer
	MembershipCommittedIdx uint64
	Size                   uint64
}

func NewSnapshot(
	dir string,
	logger hclog.Logger,
	committedPeers []*Peer,
	committedIdx uint64,
) (*Snapshot, error) {
	snap := &Snapshot{
		dir:    dir,
		logger: logger,
	}
	return snap, nil
}

type SnapshotStore struct {
	dir       string
	numRetain int
	logger    hclog.Logger
}

func NewSnapshotStore(
	dir string,
	numRetain int,
	logger hclog.Logger,
) (*SnapshotStore, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	s := &SnapshotStore{
		dir:       dir,
		numRetain: numRetain,
		logger:    logger,
	}
	return s, nil
}

func (s *SnapshotStore) loadMeta(name string) (*SnapshotMeta, error) {
	path := filepath.Join(s.dir, name, metafileName)
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	buf := bufio.NewReader(fh)
	dec := json.NewDecoder(buf)
	meta := &SnapshotMeta{}
	if err := dec.Decode(meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (s *SnapshotStore) browse() ([]*SnapshotMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		s.logger.Error("failed to scan snapshot directory", "error", err)
	}

	res := []*SnapshotMeta{}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), tmpSuffix) {
			s.logger.Warn("found temparary snapshot", "name", e.Name())
			continue
		}
		meta, err := s.loadMeta(e.Name())
		if err != nil {
			s.logger.Warn("failed to load metadata", "name", e.Name(), "error", err)
		}
		res = append(res, meta)
	}
	// latest snapshots is at the top
	sort.Slice(res, func(i, j int) bool {
		if res[i].Term == res[j].Term {
			if res[i].Idx == res[j].Idx {
				return res[i].Path > res[j].Path
			}
			return res[i].Idx > res[j].Idx
		}
		return res[i].Term > res[j].Term
	})
	return res, nil
}
