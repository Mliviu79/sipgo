package sip

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testCreateAddr(t *testing.T, addr string) Addr {
	a := Addr{}
	require.NoError(t, a.parseAddr(addr))
	return a
}

func TestIntegrationTransactionLayerServerTx(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}

	tp := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	txl := NewTransactionLayer(tp)

	req := testCreateRequest(t, "OPTIONS", "sip:192.168.0.1", "UDP", "127.0.0.1:15069")
	key, _ := ServerTxKeyMake(req)

	var count int32 = 0
	txl.OnRequest(func(req *Request, tx *ServerTx) {
		atomic.AddInt32(&count, 1)
		t.Log("Request")
	})

	// Connection will be created
	err := txl.handleRequest(req)
	require.NoError(t, err)

	// Now create connection and test multiple concurent received request
	tp.udp.CreateConnection(context.TODO(),
		testCreateAddr(t, "127.0.0.1:15069"),
		testCreateAddr(t, "192.168.0.1:1234"),
		tp.handleMessage,
	)

	wg := sync.WaitGroup{}
	wg.Add(3)
	for range []int{0, 1, 2} {
		go func() {
			defer wg.Done()
			err := txl.handleRequest(req)
			if err != nil {
				t.Log("Request failed with err", err)
			}
		}()
	}

	wg.Wait()
	require.EqualValues(t, 1, atomic.LoadInt32(&count))
	require.EqualValues(t, 1, len(txl.serverTransactions.items))

	// After termination of transaction, it  must be removed from list
	tx := txl.serverTransactions.items[key]
	require.NotNil(t, tx)
	tx.Terminate()
	require.EqualValues(t, 0, len(txl.serverTransactions.items))
}

func TestTransactionLayerMalformedRequestStateless400(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}

	// Listen on a UDP port to receive the stateless 400 response.
	receiverAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	require.NoError(t, err)
	receiverConn, err := net.ListenUDP("udp", receiverAddr)
	require.NoError(t, err)
	defer receiverConn.Close()
	receiverActualAddr := receiverConn.LocalAddr().String()

	// Set up the transaction layer with a real transport.
	tp := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	txl := NewTransactionLayer(tp)

	var handlerCalled int32
	txl.OnRequest(func(req *Request, tx *ServerTx) {
		atomic.AddInt32(&handlerCalled, 1)
	})

	// Create a UDP connection in the transport pool so WriteMsg can find it.
	localAddr := "127.0.0.1:15071"
	_, err = tp.udp.CreateConnection(
		context.TODO(),
		testCreateAddr(t, localAddr),
		testCreateAddr(t, receiverActualAddr),
		tp.handleMessage,
	)
	require.NoError(t, err)

	// Build a malformed request: valid Via, From, To, Call-ID, but NO CSeq.
	raw := strings.Join([]string{
		"REGISTER sip:192.168.100.30:5060 SIP/2.0",
		"Via: SIP/2.0/UDP " + receiverActualAddr + ";branch=z9hG4bK-test123",
		"From: <sip:alice@example.com>;tag=from1",
		"To: <sip:alice@example.com>",
		"Call-ID: malformed-test-call-id",
		"Content-Length: 0",
		"",
		"",
	}, "\r\n")

	msg, err := ParseMessage([]byte(raw))
	require.NoError(t, err)

	req := msg.(*Request)
	req.SetTransport("UDP")
	req.SetSource(receiverActualAddr)

	// handleRequest should return an error because CSeq is missing.
	err = txl.handleRequest(req)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CSeq")
	assert.ErrorIs(t, err, errMalformedRequest)

	// The request handler should NOT have been called.
	assert.EqualValues(t, 0, atomic.LoadInt32(&handlerCalled))

	// Read the stateless 400 response that should have been sent.
	buf := make([]byte, 4096)
	receiverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, readErr := receiverConn.ReadFromUDP(buf)
	require.NoError(t, readErr, "expected to receive a stateless 400 response")

	respMsg, err := ParseMessage(buf[:n])
	require.NoError(t, err)

	resp, ok := respMsg.(*Response)
	require.True(t, ok, "expected a SIP response")
	assert.Equal(t, 400, resp.StatusCode)
	assert.Equal(t, "Missing CSeq Header Field", resp.Reason)
}

// TestTransactionLayerMalformedRequestReason pins the stateless 400's reason
// phrase to makeServerTxKey: every header the key cannot be built without gets
// its own phrase, and a request the key accepts gets the plain fallback.
func TestTransactionLayerMalformedRequestReason(t *testing.T) {
	const (
		rfc3261Via = "Via: SIP/2.0/UDP 10.0.0.1:5060;branch=z9hG4bKreason1"
		rfc2543Via = "Via: SIP/2.0/UDP 10.0.0.1:5060;branch=reason1"
		from       = "From: <sip:alice@example.com>;tag=from1"
		fromNoTag  = "From: <sip:alice@example.com>"
		callID     = "Call-ID: reason-call-id"
		cseq       = "CSeq: 1 OPTIONS"
	)

	tests := []struct {
		name    string
		headers []string
		reason  string
	}{
		{name: "missing Via", headers: []string{from, callID, cseq}, reason: "Missing Via Header Field"},
		{name: "missing CSeq", headers: []string{rfc3261Via, from, callID}, reason: "Missing CSeq Header Field"},
		{name: "missing From without RFC 3261 branch", headers: []string{rfc2543Via, callID, cseq}, reason: "Missing From Header Field"},
		{name: "missing From tag without RFC 3261 branch", headers: []string{rfc2543Via, fromNoTag, callID, cseq}, reason: "Missing From Tag"},
		{name: "missing Call-ID without RFC 3261 branch", headers: []string{rfc2543Via, from, cseq}, reason: "Missing Call-ID Header Field"},
		{name: "missing From with RFC 3261 branch", headers: []string{rfc3261Via, callID, cseq}, reason: "Bad Request"},
		{name: "well formed with RFC 3261 branch", headers: []string{rfc3261Via, from, callID, cseq}, reason: "Bad Request"},
		{name: "well formed without RFC 3261 branch", headers: []string{rfc2543Via, from, callID, cseq}, reason: "Bad Request"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := append([]string{"OPTIONS sip:bob@example.com SIP/2.0"}, tc.headers...)
			raw = append(raw, "To: <sip:bob@example.com>", "Content-Length: 0", "", "")
			req := testCreateMessage(t, raw).(*Request)

			_, keyErr := makeServerTxKey(req, "")
			if tc.reason == "Bad Request" {
				require.NoError(t, keyErr)
			} else {
				require.Error(t, keyErr)
			}
			assert.Equal(t, tc.reason, malformedRequestReason(req))
		})
	}
}

func TestTransactionLayerClientTx(t *testing.T) {
	if os.Getenv("TEST_INTEGRATION") == "" {
		t.Skip("Use TEST_INTEGRATION env value to run this test")
		return
	}
	tp := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	txl := NewTransactionLayer(tp)

	req := testCreateRequest(t, "OPTIONS", "sip:127.0.0.1:9876", "UDP", "127.0.0.1:15070")

	// Each goroutine sends a copy of the request, which has the same
	// transaction key: the transport layer writes into the request it sends.
	// The results are checked on the test goroutine.
	type result struct {
		sent *Request
		tx   *ClientTx
		err  error
	}
	const requests = 3
	results := make(chan result, requests)
	for range requests {
		go func(sent *Request) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			tx, err := txl.Request(ctx, sent)
			results <- result{sent: sent, tx: tx, err: err}
		}(req.Clone())
	}

	count := 0
	for i := range requests {
		select {
		case r := <-results:
			if r.err != nil {
				t.Log("Request failed with err", r.err)
				continue
			}
			count++
			require.Same(t, r.sent, r.tx.origin)
		case <-time.After(10 * time.Second):
			t.Fatalf("%d of %d requests did not return", requests-i, requests)
		}
	}
	// Only one transaction will be created and executed
	require.Equal(t, 1, count)
	require.Equal(t, 2, tp.udp.pool.Size())
	assert.True(t, tp.udp.pool.Get("127.0.0.1:9876") != nil)
}

// lockedBuffer collects the bytes a server transaction writes. Writes can come
// from a transaction timer goroutine, so reads and writes share a mutex.
type lockedBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestTransactionLayerRecoversPanickingHandler drives the one place the layer
// hands a request to user code with a real server transaction. A panic that
// escapes it is caught by the test itself, so a missing recover shows up as an
// assertion failure rather than as a crashed test binary.
func TestTransactionLayerRecoversPanickingHandler(t *testing.T) {
	const sentinel = "sipgo test handler panic"

	newLayer := func(t *testing.T, handler TransactionRequestHandler) (*TransactionLayer, *logCapture) {
		t.Helper()
		capture := &logCapture{}
		tpl := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
		t.Cleanup(func() { _ = tpl.Close() })
		txl := NewTransactionLayer(tpl, WithTransactionLayerLogger(slog.New(capture)))
		txl.OnRequest(handler)
		return txl, capture
	}
	newTx := func(t *testing.T, req *Request) (*ServerTx, *lockedBuffer) {
		t.Helper()
		outgoing := &lockedBuffer{}
		conn := &UDPConnection{
			PacketConn: &fakes.UDPConn{
				Reader:  strings.NewReader(""),
				Writers: map[string]io.Writer{"127.0.0.1:5060": outgoing},
			},
		}
		// The reference serverRequestConnection takes for a new transaction,
		// released when the transaction ends.
		conn.Ref(1)
		tx := NewServerTx("panic-"+req.Method.String(), req, conn, slog.New(&logCapture{}))
		require.NoError(t, tx.Init())
		return tx, outgoing
	}
	run := func(txl *TransactionLayer, req *Request, tx *ServerTx) (escaped any) {
		defer func() { escaped = recover() }()
		txl.runRequestHandler(req, tx)
		return nil
	}
	waitDone := func(t *testing.T, tx *ServerTx) {
		t.Helper()
		select {
		case <-tx.Done():
		case <-time.After(2 * time.Second):
			t.Fatal("transaction did not end after the recovered panic")
		}
	}

	t.Run("unanswered request gets 500 and the transaction ends", func(t *testing.T) {
		txl, capture := newLayer(t, func(req *Request, tx *ServerTx) {
			panic(sentinel)
		})
		req := testCreateRequest(t, "OPTIONS", "sip:example.com", "TCP", "127.0.0.1:5060")
		tx, outgoing := newTx(t, req)

		escaped := run(txl, req, tx)
		require.Nil(t, escaped, "the handler panic escaped the transaction layer")

		assert.Contains(t, outgoing.String(), "SIP/2.0 500 Server Internal Error")
		waitDone(t, tx)

		rec := capture.find(t, "Request handler panicked")
		assert.Equal(t, slog.LevelError, rec.Level)
		fields := flatten(rec)
		assert.Contains(t, fields, "method=OPTIONS")
		assert.Contains(t, fields, "callid="+req.CallID().Value())
		assert.Contains(t, fields, "panic="+sentinel)
		var stack string
		rec.Attrs(func(a slog.Attr) bool {
			if a.Key == "stack" {
				stack = a.Value.String()
			}
			return true
		})
		assert.Contains(t, stack, "TestTransactionLayerRecoversPanickingHandler")
		assert.Contains(t, fields, "tx="+tx.Key())
	})

	t.Run("final response already sent is kept", func(t *testing.T) {
		txl, _ := newLayer(t, func(req *Request, tx *ServerTx) {
			if err := tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)); err != nil {
				t.Errorf("respond 200: %v", err)
			}
			panic(sentinel)
		})
		req := testCreateRequest(t, "OPTIONS", "sip:example.com", "TCP", "127.0.0.1:5060")
		tx, outgoing := newTx(t, req)

		escaped := run(txl, req, tx)
		require.Nil(t, escaped, "the handler panic escaped the transaction layer")

		tx.fsmMu.Lock()
		stored := tx.fsmResp
		tx.fsmMu.Unlock()
		require.NotNil(t, stored)
		assert.Equal(t, StatusOK, stored.StatusCode, "the final response retransmissions are answered with")
		assert.Contains(t, outgoing.String(), "SIP/2.0 200")
		assert.NotContains(t, outgoing.String(), "SIP/2.0 500")
		waitDone(t, tx)
	})

	t.Run("provisional only still gets 500", func(t *testing.T) {
		txl, _ := newLayer(t, func(req *Request, tx *ServerTx) {
			if err := tx.Respond(NewResponseFromRequest(req, StatusRinging, "Ringing", nil)); err != nil {
				t.Errorf("respond 180: %v", err)
			}
			panic(sentinel)
		})
		req, _, _ := testCreateInvite(t, "sip:example.com", "TCP", "127.0.0.1:5060")
		tx, outgoing := newTx(t, req)

		escaped := run(txl, req, tx)
		require.Nil(t, escaped, "the handler panic escaped the transaction layer")

		assert.Contains(t, outgoing.String(), "SIP/2.0 180")
		assert.Contains(t, outgoing.String(), "SIP/2.0 500 Server Internal Error")
		waitDone(t, tx)
	})
}
