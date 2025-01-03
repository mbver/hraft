package hraft

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNetTransport_AppendEntries(t *testing.T) {
	addresses := newTestAddressesWithSameIP()
	defer addresses.cleanup()
	addr1 := addresses.next()
	trans1, err := newTestTransport(addr1)
	require.Nil(t, err)
	defer trans1.Close()
	addr2 := addresses.next()
	trans2, err := newTestTransport(addr2)
	require.Nil(t, err)
	req := &AppendEntriesRequest{
		Term:        10,
		Leader:      []byte(addr1),
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
	resp := AppendEntriesResponse{
		Term:               4,
		LastLogIdx:         90,
		Success:            true,
		PrevLogCheckFailed: false,
	}
	trans1.connectPeer(addr2)
	go func() {
		rpc := <-trans2.RpcCh()
		require.True(t, reflect.DeepEqual(rpc.command, req), "command mismatch")
		rpc.respCh <- &resp
	}()
	var gotResp AppendEntriesResponse
	trans1.AppendEntries(addr2, req, &gotResp)
	require.True(t, reflect.DeepEqual(resp, gotResp), "response mismatch")
}
