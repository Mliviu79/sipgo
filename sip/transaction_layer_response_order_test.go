//go:build go1.25

package sip

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/stretchr/testify/require"
)

// TestTransactionLayerClientResponsesInOrder has a transport hand the
// transaction layer two provisional responses and then a 2xx to one INVITE,
// back to back, as a connection's read loop does. The client transaction
// passes every one of them to the TU in the order they were received (RFC
// 3261 section 17.1.1.2), however late the TU reads them.
func TestTransactionLayerClientResponsesInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		txl := NewTransactionLayer(NewTransportLayer(net.DefaultResolver, NewParser(), nil))

		req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "udp", "127.0.0.2:5060")
		req.raddr = Addr{IP: net.ParseIP("127.0.0.99"), Port: 5060}
		conn := &UDPConnection{
			PacketConn: &fakes.UDPConn{Writers: map[string]io.Writer{"127.0.0.99:5060": io.Discard}},
		}
		key, err := ClientTxKeyMake(req)
		require.NoError(t, err)
		tx := NewClientTx(key, req, conn, slog.Default())
		txl.clientTransactions.lock()
		txl.clientTransactions.items[key] = tx
		tx.OnTerminate(txl.clientTxTerminate)
		txl.clientTransactions.unlock()
		require.NoError(t, tx.Init())
		defer tx.Terminate()

		received := []*Response{
			NewResponseFromRequest(req, StatusSessionInProgress, "First Progress", nil),
			NewResponseFromRequest(req, StatusSessionInProgress, "Second Progress", nil),
			NewResponseFromRequest(req, StatusOK, "OK", nil),
		}
		for _, res := range received {
			txl.handleMessage(res)
		}

		var passed []string
		for range received {
			select {
			case res := <-tx.Responses():
				passed = append(passed, res.Reason)
			case <-time.After(time.Second):
			}
		}
		require.Equal(t, []string{"First Progress", "Second Progress", "OK"}, passed,
			"the responses passed to the TU")
	})
}
