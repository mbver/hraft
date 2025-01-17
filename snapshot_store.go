package hraft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	metafileName  = "meta.json"
	tmpSuffix     = ".tmp"
	stateFileName = "state.bin"
)

type Snapshot struct {
	dir       string
	stateFile string
	buf       *bufio.Writer
	logger    hclog.Logger
	closed    bool
}

type SnapshotMeta struct {
	Name                   string
	Term                   uint64
	Idx                    uint64
	Peers                  []*Peer
	MembershipCommittedIdx uint64
	Size                   uint64
	CRC                    []byte
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

func (s *SnapshotStore) listAll() ([]*SnapshotMeta, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		s.logger.Error("failed to scan snapshot directory", "error", err)
		return nil, err
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
			continue
		}
		res = append(res, meta)
	}
	// latest snapshots is on top
	sort.Slice(res, func(i, j int) bool {
		if res[i].Term == res[j].Term {
			if res[i].Idx == res[j].Idx {
				return res[i].Name > res[j].Name
			}
			return res[i].Idx > res[j].Idx
		}
		return res[i].Term > res[j].Term
	})
	return res, nil
}

func (s *SnapshotStore) List() ([]*SnapshotMeta, error) {
	all, err := s.listAll()
	if err != nil {
		s.logger.Error("failed to list snapshots", "error", err)
		return nil, err
	}
	res := make([]*SnapshotMeta, 0, s.numRetain)
	for i := 0; i < len(all) && i < s.numRetain; i++ {
		res = append(res, all[i])
	}
	return res, nil
}

func (s *SnapshotStore) Reap() error {
	all, err := s.listAll()
	if err != nil {
		s.logger.Error("failed to list snapshots", "error", err)
		return err
	}
	for i := s.numRetain; i < len(all); i++ {
		path := filepath.Join(s.dir, all[i].Name)
		s.logger.Info("reaping snapshot", "path", path)
		if err := os.RemoveAll(path); err != nil {
			s.logger.Error("failed to reap snapshot", "path", path, "error", err)
			return err
		}
	}
	return nil
}

type bufferedReader struct {
	buf *bufio.Reader
	fh  *os.File
}

func newBufferedReader(fh *os.File) *bufferedReader {
	return &bufferedReader{
		buf: bufio.NewReader(fh),
		fh:  fh,
	}
}

func (b *bufferedReader) Read(p []byte) (n int, err error) {
	return b.buf.Read(p)
}

func (b *bufferedReader) Close() error {
	return b.fh.Close()
}

func (s *SnapshotStore) OpenSnapshot(name string) (*SnapshotMeta, *bufferedReader, error) {
	meta, err := s.loadMeta(name)
	if err != nil {
		s.logger.Error("failed to get meta data to open snaphshot", "error", err)
		return nil, nil, err
	}
	fh, err := os.Open(filepath.Join(s.dir, name, stateFileName))
	if err != nil {
		s.logger.Error("failed to open state file", "error", err)
		return nil, nil, err
	}
	crcHash := crc64.New(crc64.MakeTable(crc64.ECMA))
	_, err = io.Copy(crcHash, fh)
	if err != nil {
		s.logger.Error("failed to read state file for crc", "error", err)
		fh.Close()
		return nil, nil, err
	}
	checksum := crcHash.Sum(nil)
	if !bytes.Equal(meta.CRC, checksum) {
		s.logger.Error("CRC mismatch", "stored", meta.CRC, "computed", checksum)
		fh.Close()
		return nil, nil, fmt.Errorf("CRC mismatch")
	}
	if _, err := fh.Seek(0, 0); err != nil {
		s.logger.Error("state file seek failed", "error", err)
		fh.Close()
		return nil, nil, err
	}
	return meta, newBufferedReader(fh), nil
}
