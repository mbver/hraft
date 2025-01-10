package hraft

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

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
		Timeout:      500 * time.Millisecond,
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

func defaultTestConfig(addr string) *Config {
	return &Config{
		LocalID:            addr,
		ElectionTimeout:    100 * time.Millisecond,
		HeartbeatTimeout:   100 * time.Millisecond,
		CommitSyncInterval: 20 * time.Millisecond,
		MaxAppendEntries:   64,
	}
}

type cluster struct {
	wg    sync.WaitGroup
	rafts []*Raft
}

func (c *cluster) add(raft *Raft) {
	c.rafts = append(c.rafts, raft)
	c.wg.Add(1)
}

func (c *cluster) close() {
	for _, raft := range c.rafts {
		go func() {
			defer c.wg.Done()
			raft.Shutdown()
		}()
	}
	c.wg.Wait()
}

func (c *cluster) getNodesByState(state RaftStateType) []*Raft {
	res := []*Raft{}
	for _, r := range c.rafts {
		if r.getStateType() == state {
			res = append(res, r)
		}
	}
	return res
}

type discardCommandsApplier struct{}

func (a *discardCommandsApplier) ApplyCommands(commits []*Commit) {
	for _, c := range commits {
		trySendErr(c.ErrCh, nil)
	}
}

type discardMembershipApplier struct{}

func (m *discardMembershipApplier) ApplyMembership(c *Commit) {
	trySendErr(c.ErrCh, nil)
}

func createTestCluster(n int, noElect bool) (*cluster, func(), error) {
	addrSource := newTestAddressesWithSameIP()
	addresses := make([]string, n)
	for i := 0; i < n; i++ {
		addresses[i] = addrSource.next()
	}
	cluster := &cluster{}
	cleanup := combineCleanup(cluster.close, addrSource.cleanup)
	var first *Raft
	for i, addr := range addresses {
		b := &RaftBuilder{}

		conf := defaultTestConfig(addr)
		conf.NoElect = noElect
		b.WithConfig(conf)

		b.WithTransportConfig(testTransportConfigFromAddr(addr))

		b.WithLogStore(newInMemLogStore())
		b.WithKVStore(newInMemKVStore())

		b.WithLogger(newTestLogger(addr))

		b.WithAppState(NewAppState(&discardCommandsApplier{}, &discardMembershipApplier{}, 1))

		raft, err := b.Build()
		if err != nil {
			return nil, cleanup, err
		}
		cluster.add(raft)
		if i == 0 {
			first = raft
			if err := first.Bootstrap(1000 * time.Millisecond); err != nil {
				return nil, cleanup, err
			}
			continue
		}
		if err := first.AddVoter(addr, 200*time.Millisecond); err != nil {
			return nil, cleanup, err
		}
		success, msg := retry(5, func() (bool, string) {
			time.Sleep(100 * time.Millisecond)
			if raft.membership.isLocalVoter() {
				return true, ""
			}
			return false, "unable to become voter"
		})
		if !success {
			return nil, cleanup, fmt.Errorf("%s", msg)
		}
	}
	return cluster, cleanup, nil
}

func retry(n int, f func() (bool, string)) (success bool, msg string) {
	for i := 0; i < n; i++ {
		success, msg = f()
		if success {
			return
		}
	}
	return
}
