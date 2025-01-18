package hraft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	snapStoreDirName = "snapshots"
	metafileName     = "meta.json"
	tmpSuffix        = ".tmp"
	stateFileName    = "state.bin"
)

type Snapshot struct {
	dir       string
	meta      *SnapshotMeta
	stateFile *os.File
	crcHash   hash.Hash64
	buf       *bufio.Writer
	logger    hclog.Logger
	closed    bool
}

func (s *Snapshot) saveMeta() error {
	path := filepath.Join(s.dir, metafileName)
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	buf := bufio.NewWriter(fh)
	enc := json.NewEncoder(buf)
	if err = enc.Encode(&s.meta); err != nil {
		return err
	}
	if err = fh.Sync(); err != nil {
		return err
	}
	return nil
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

type SnapshotStore struct {
	dir       string
	numRetain int
	logger    hclog.Logger
}

func defaultSnapshotLogger() hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   "snapshot",
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})
}

func canCreateFile(baseDir string) error {
	path := filepath.Join(baseDir, "test")
	fh, err := os.Create(path)
	if err != nil {
		return err
	}
	if err = fh.Close(); err != nil {
		return err
	}
	if err = os.Remove(path); err != nil {
		return err
	}
	return nil
}

func NewSnapshotStore(
	baseDir string,
	numRetain int,
	logger hclog.Logger,
) (*SnapshotStore, error) {
	if numRetain < 1 {
		return nil, fmt.Errorf("must retain at least 1 snapshot")
	}
	if logger == nil {
		logger = defaultSnapshotLogger()
	}
	dir := filepath.Join(baseDir, snapStoreDirName)
	if err := os.MkdirAll(dir, 0755); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("failed to create snapshot store dir %w", err)
	}
	if err := canCreateFile(dir); err != nil {
		return nil, fmt.Errorf("unable to create file in snapshot store dir %s: %w", dir, err)
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
	if _, err := fh.Seek(0, io.SeekStart); err != nil {
		s.logger.Error("state file seek failed", "error", err)
		fh.Close()
		return nil, nil, err
	}
	return meta, newBufferedReader(fh), nil
}

func toSnapshotName(term, idx uint64) string {
	ms := time.Now().UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("%d-%d-%d", term, idx, ms)
}

func (s *SnapshotStore) CreateSnapshot(
	idx, term uint64,
	peers []*Peer,
	memsCommitIdx uint64,
) (*Snapshot, error) {
	name := toSnapshotName(term, idx)
	dir := filepath.Join(s.dir, name)
	s.logger.Info("creating new snapshot", "path", dir)

	if err := os.MkdirAll(dir, 0755); err != nil {
		s.logger.Error("failed to create snapshot dir", "error", err)
		return nil, err
	}
	snap := &Snapshot{
		dir:    dir,
		logger: s.logger,
		meta: &SnapshotMeta{
			Name:                   name,
			Idx:                    idx,
			Term:                   term,
			Peers:                  peers,
			MembershipCommittedIdx: memsCommitIdx,
		},
	}
	if err := snap.saveMeta(); err != nil {
		s.logger.Error("failed to write metadata", "error", err)
		return nil, err
	}
	stateFile, err := os.Create(filepath.Join(dir, stateFileName))
	if err != nil {
		s.logger.Error("failed to create statefile", "error", err)
		return nil, err
	}
	snap.stateFile = stateFile
	snap.crcHash = crc64.New(crc64.MakeTable(crc64.ECMA))

	w := io.MultiWriter(snap.stateFile, snap.crcHash)
	snap.buf = bufio.NewWriter(w)

	return snap, nil
}
