package hraft

import (
	"fmt"
	"net"
	"strconv"
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

func testTransportConfigFromAddr(addr string) *netTransportConfig {
	return &netTransportConfig{
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
