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

func twoTestTransport() (*netTransport, *netTransport, func(), error) {
	addresses := newTestAddressesWithSameIP()
	addr1 := addresses.next()
	trans1, err := newTestTransport(addr1)
	if err != nil {
		return nil, nil, addresses.cleanup, err
	}
	addr2 := addresses.next()
	trans2, err := newTestTransport(addr2)
	if err != nil {
		return nil, nil, addresses.cleanup, err
	}
	return trans1, trans2, func() {
		trans2.Close()
		trans1.Close()
		addresses.cleanup()
	}, nil
}
