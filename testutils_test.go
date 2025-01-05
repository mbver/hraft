package hraft

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/hashicorp/go-hclog"
	"github.com/mbver/mlist/testaddr"
)

// utils for testing
type testAddressesWithSameIP struct {
	ip      net.IP
	port    int
	cleanup func()
}

func newTestAddressesWithSameIP() *testAddressesWithSameIP {
	ip, cleanup := testaddr.BindAddrs.NextAvailAddr()
	return &testAddressesWithSameIP{
		ip:      ip,
		port:    7944,
		cleanup: cleanup,
	}
}

func (a *testAddressesWithSameIP) next() string {
	a.port++
	return net.JoinHostPort(a.ip.String(), strconv.Itoa(a.port))
}

func testTransportConfigFromAddr(addr string) *NetTransportConfig {
	return &NetTransportConfig{
		BindAddr:     addr,
		Timeout:      2 * time.Second,
		TimeoutScale: DefaultTimeoutScale,
		PoolSize:     2,
	}
}

func newTestLogger(name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Name:   name,
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})
}

func newTestTransport(addr string) (*netTransport, error) {
	config := testTransportConfigFromAddr(addr)
	logger := newTestLogger(fmt.Sprintf("transport:%s", addr))
	return newNetTransport(config, logger)
}

func combineCleanup(cleanups ...func()) func() {
	return func() {
		for _, f := range cleanups {
			f()
		}
	}
}

func newTestTransportWithLogger(logger hclog.Logger) (*netTransport, func(), error) {
	addresses := newTestAddressesWithSameIP()
	cleanup1 := addresses.cleanup
	addr := addresses.next()
	config := testTransportConfigFromAddr(addr)
	trans, err := newNetTransport(config, logger)
	if err != nil {
		return nil, cleanup1, err
	}
	return trans, combineCleanup(cleanup1, trans.Close), nil
}

func twoTestTransport() (*netTransport, *netTransport, func(), error) {
	addresses := newTestAddressesWithSameIP()
	cleanup1 := addresses.cleanup
	addr1 := addresses.next()
	trans1, err := newTestTransport(addr1)
	if err != nil {
		return nil, nil, cleanup1, err
	}
	cleanup2 := combineCleanup(cleanup1, trans1.Close)
	addr2 := addresses.next()
	trans2, err := newTestTransport(addr2)
	if err != nil {
		return nil, nil, cleanup2, err
	}
	return trans1, trans2, combineCleanup(cleanup2, trans2.Close), nil
}

func getTestAppendEntriesRequestResponse(leader string) (*AppendEntriesRequest, *AppendEntriesResponse) {
	req := &AppendEntriesRequest{
		Term:        10,
		Leader:      []byte(leader),
		PrevLogIdx:  100,
		PrevLogTerm: 4,
		Entries: []*Log{
			{
				Idx:  101,
				Term: 4,
				Type: LogNoOp,
			},
		},
		LeaderCommitIdx: 90,
	}
	resp := &AppendEntriesResponse{
		Term:               4,
		LastLogIdx:         90,
		Success:            true,
		PrevLogCheckFailed: false,
	}
	return req, resp
}

func tryGetNotify(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

var testBoltStore *BoltStore

func TestMain(t *testing.M) {
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		panic(err)
	}
	defer os.Remove(fh.Name())

	testBoltStore, err = NewBoltStore(fh.Name(), nil, nil)
	if err != nil {
		panic(err)
	}
	t.Run()
}

func newTestStoreWithOpts(opts *bolt.Options) (*BoltStore, func(), error) {
	cleanup := func() {}
	fh, err := os.CreateTemp("", "bolt")
	if err != nil {
		return nil, cleanup, err
	}
	cleanup = func() { os.Remove(fh.Name()) }
	store, err := NewBoltStore(fh.Name(), opts, nil)
	if err != nil {
		return nil, cleanup, err
	}
	cleanup1 := combineCleanup(func() { store.Close() }, cleanup)
	return store, cleanup1, nil
}

func createTestLog(idx uint64, data string) *Log {
	return &Log{
		Idx:  idx,
		Data: []byte(data),
	}
}

func defaultTestConfig(addr string, peers []string) *Config {
	return &Config{
		TransportConfig: testTransportConfigFromAddr(addr),
		InitalPeers:     peers,
	}
}

type cluster struct {
	rafts []*Raft
}

func (c *cluster) add(raft *Raft) {
	c.rafts = append(c.rafts, raft)
}

func createTestCluster(n int) (*cluster, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	cleanup := addrSource.cleanup
	addresses := make([]string, n)
	var cluster *cluster
	for i := 0; i < n; i++ {
		addresses[i] = addrSource.next()
	}

	for _, addr := range addresses {
		currentCleanup := cleanup
		b := &RaftBuilder{}
		conf := defaultTestConfig(addr, addresses)
		b.WithConfig(conf)
		logStore, err := NewLogStore(testBoltStore, []byte("log_"+addr))
		if err != nil {
			return nil, cleanup, err
		}
		b.WithLogStore(logStore)

		kvStore, err := NewKVStore(testBoltStore, []byte("kv_"+addr))
		if err != nil {
			return nil, cleanup, err
		}
		b.WithKVStore(kvStore)

		b.WithLogger(newTestLogger(addr))

		raft, err := b.Build()
		if err != nil {
			return nil, cleanup, err
		}
		cluster.add(raft)
		cleanup = combineCleanup(raft.Shutdown, currentCleanup)
	}
	return cluster, cleanup, nil
}
