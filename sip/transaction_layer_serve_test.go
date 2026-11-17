package sip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type serveCtxKey struct{}

var errServeTestStop = errors.New("sip test: serving stopped")

// servedLayer is a transaction layer reading requests from a real UDP
// listener, with a client socket to send them from and read the answers on.
type servedLayer struct {
	txl        *TransactionLayer
	tpl        *TransportLayer
	client     net.PacketConn
	serverAddr *net.UDPAddr
}

func newServedLayer(t *testing.T, opts ...TransactionLayerOption) *servedLayer {
	t.Helper()
	serverConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	client, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)

	tpl := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	txl := NewTransactionLayer(tpl, opts...)

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		_ = tpl.ServeUDP(serverConn)
	}()
	t.Cleanup(func() {
		txl.Close()
		_ = tpl.Close()
		_ = serverConn.Close()
		<-readerDone
		_ = client.Close()
	})
	return &servedLayer{txl: txl, tpl: tpl, client: client, serverAddr: serverConn.LocalAddr().(*net.UDPAddr)}
}

// send writes one request of method to the layer and returns its Call-ID.
func (s *servedLayer) send(t *testing.T, method string) string {
	t.Helper()
	callID, err := s.write(method)
	require.NoError(t, err)
	return callID
}

// write sends one request of method and returns its Call-ID. It reports a
// failure as an error so a goroutine other than the test's can use it.
func (s *servedLayer) write(method string) (string, error) {
	callID := fmt.Sprintf("serve-%d", time.Now().UnixNano())
	clientAddr := s.client.LocalAddr().String()
	raw := strings.Join([]string{
		fmt.Sprintf("%s sip:server@%s SIP/2.0", method, s.serverAddr),
		"Via: SIP/2.0/UDP " + clientAddr + ";branch=" + GenerateBranch(),
		"From: <sip:alice@127.0.0.1>;tag=" + GenerateTagN(8),
		"To: <sip:server@127.0.0.1>",
		"Call-ID: " + callID,
		"CSeq: 1 " + method,
		"Content-Length: 0",
		"", "",
	}, "\r\n")
	_, err := s.client.WriteTo([]byte(raw), s.serverAddr)
	return callID, err
}

// receive reads one response, or fails after the wait.
func (s *servedLayer) receive(t *testing.T, wait time.Duration) *Response {
	t.Helper()
	res, ok := s.tryReceive(t, wait)
	require.True(t, ok, "no response within %s", wait)
	return res
}

func (s *servedLayer) tryReceive(t *testing.T, wait time.Duration) (*Response, bool) {
	t.Helper()
	buf := make([]byte, 65535)
	require.NoError(t, s.client.SetReadDeadline(time.Now().Add(wait)))
	n, _, err := s.client.ReadFrom(buf)
	if err != nil {
		var netErr net.Error
		require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "read: %v", err)
		return nil, false
	}
	msg, err := ParseMessage(buf[:n])
	require.NoError(t, err)
	res, ok := msg.(*Response)
	require.True(t, ok, "expected a response, got %q", buf[:n])
	return res, true
}

// serve starts ServeRequests on ctx and returns its ready and result channels.
func serve(txl *TransactionLayer, ctx context.Context) (ready <-chan struct{}, result <-chan error) {
	r := make(chan struct{})
	e := make(chan error, 1)
	go func() {
		e <- txl.ServeRequests(ctx, func() { close(r) })
	}()
	return r, e
}

func respondOK(ctx context.Context, req *Request, tx *ServerTx) {
	_ = tx.Respond(ctx, NewResponseFromRequest(req, StatusOK, "OK", nil))
}

func TestServeRequestsHandlerReceivesRunContext(t *testing.T) {
	l := newServedLayer(t)
	seen := make(chan any, 1)
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {
		seen <- ctx.Value(serveCtxKey{})
		respondOK(ctx, req, tx)
	})

	ctx, cancel := context.WithCancelCause(context.WithValue(testCtx(t), serveCtxKey{}, "run"))
	ready, result := serve(l.txl, ctx)
	t.Cleanup(func() {
		cancel(errServeTestStop)
		<-result
	})
	<-ready

	l.send(t, "OPTIONS")
	assert.Equal(t, StatusOK, l.receive(t, 5*time.Second).StatusCode)
	assert.Equal(t, "run", <-seen)
}

// noStackWith fails when a goroutine still runs a function whose name contains
// any of the fragments.
func noStackWith(t *testing.T, fragments ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		leaked := ""
		for _, f := range fragments {
			if strings.Contains(stacks, f) {
				leaked = f
				break
			}
		}
		if leaked == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutine still inside %q:\n%s", leaked, stacks)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServeRequestsReturnsNilAfterCloseOnceHandlersReturned(t *testing.T) {
	l := newServedLayer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var returned atomic.Bool
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {
		close(entered)
		<-release
		returned.Store(true)
	})

	_, result := serve(l.txl, testCtx(t))
	l.send(t, "OPTIONS")
	<-entered

	l.txl.Close()
	select {
	case err := <-result:
		t.Fatalf("ServeRequests returned %v while a handler it admitted was running", err)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		require.NoError(t, err)
		assert.True(t, returned.Load(), "ServeRequests returned before the handler")
	case <-time.After(5 * time.Second):
		t.Fatal("ServeRequests did not return after the handler returned")
	}
	noStackWith(t, "(*TransactionLayer).ServeRequests", "(*TransactionLayer).runRequestHandler")
}

func TestServeRequestsAfterContextEndAnswers480(t *testing.T) {
	l := newServedLayer(t)
	var ran atomic.Bool
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {
		ran.Store(true)
		respondOK(ctx, req, tx)
	})

	ctx, cancel := context.WithCancelCause(testCtx(t))
	ready, result := serve(l.txl, ctx)
	<-ready
	cancel(errServeTestStop)
	err := <-result
	require.ErrorIs(t, err, errServeTestStop)

	l.send(t, "OPTIONS")
	res := l.receive(t, 5*time.Second)
	assert.Equal(t, StatusTemporarilyUnavailable, res.StatusCode)
	assert.False(t, ran.Load(), "the handler ran after the run's ctx ended")
}

func TestUnservedRequestIsAnswered480AndAckDropped(t *testing.T) {
	// A final answer keeps an unreliable transaction until Timer J ends it.
	savedTimerJ := Timer_J
	Timer_J = 10 * time.Millisecond
	t.Cleanup(func() { Timer_J = savedTimerJ })

	l := newServedLayer(t)
	var ran atomic.Bool
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {
		ran.Store(true)
		respondOK(ctx, req, tx)
	})

	l.send(t, "OPTIONS")
	res := l.receive(t, 5*time.Second)
	assert.Equal(t, StatusTemporarilyUnavailable, res.StatusCode)

	l.send(t, "ACK")
	_, ok := l.tryReceive(t, 300*time.Millisecond)
	assert.False(t, ok, "an ACK that meets no run must be dropped, not answered")
	assert.False(t, ran.Load())

	require.Eventually(t, func() bool {
		l.txl.serverTransactions.mu.RLock()
		defer l.txl.serverTransactions.mu.RUnlock()
		return len(l.txl.serverTransactions.items) == 0
	}, 5*time.Second, 10*time.Millisecond, "the transactions of unserved requests did not end")
}

func TestServeRequestsServesOnce(t *testing.T) {
	l := newServedLayer(t)
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {})

	ctx, cancel := context.WithCancelCause(testCtx(t))
	ready, result := serve(l.txl, ctx)
	<-ready

	err := l.txl.ServeRequests(testCtx(t), nil)
	require.ErrorIs(t, err, ErrRequestsServed, "second call while the first runs")

	cancel(errServeTestStop)
	require.ErrorIs(t, <-result, errServeTestStop)

	err = l.txl.ServeRequests(testCtx(t), nil)
	require.ErrorIs(t, err, ErrRequestsServed, "second call after the first ended")
}

func TestServeRequestsPanickingHandlerAnswered500AndRunEndsOnClose(t *testing.T) {
	l := newServedLayer(t)
	l.txl.OnRequest(func(ctx context.Context, req *Request, tx *ServerTx) {
		panic("sip test handler panic")
	})

	ready, result := serve(l.txl, testCtx(t))
	<-ready

	l.send(t, "OPTIONS")
	assert.Equal(t, StatusInternalServerError, l.receive(t, 5*time.Second).StatusCode)

	l.txl.Close()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the run did not end on close after a recovered panic")
	}
}

func TestServeRequestsManyRequestsWhileRunEnds(t *testing.T) {
	l := newServedLayer(t)
	var entered, exited atomic.Int64
	handler := func(ctx context.Context, req *Request, tx *ServerTx) {
		entered.Add(1)
		defer exited.Add(1)
		respondOK(ctx, req, tx)
	}
	l.txl.OnRequest(handler)

	ctx, cancel := context.WithCancelCause(testCtx(t))
	ready, result := serve(l.txl, ctx)
	<-ready

	var wg sync.WaitGroup
	stopInstall := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopInstall:
				return
			default:
				l.txl.OnRequest(handler)
			}
		}
	}()
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				_, err := l.write("OPTIONS")
				assert.NoError(t, err)
			}
		}()
	}

	require.Eventually(t, func() bool { return entered.Load() > 0 }, 5*time.Second, time.Millisecond)
	cancel(errServeTestStop)
	select {
	case err := <-result:
		require.ErrorIs(t, err, errServeTestStop)
	case <-time.After(10 * time.Second):
		t.Fatal("ServeRequests did not return while requests were handed off")
	}
	assert.Equal(t, entered.Load(), exited.Load(), "a handler the run admitted was still running when it returned")

	close(stopInstall)
	wg.Wait()
}

// A layer nothing serves requests with, the client-only layer, keeps today's
// behaviour: nothing is sent and the request is logged.
func TestClientOnlyLayerAnswersNothingAndLogs(t *testing.T) {
	// The package logger is global and goroutines of earlier tests read it: the
	// test reads what TestMain's logger retained and swaps nothing.
	const unhandled = "Unhandled sip request. OnRequest handler not added"
	before := packageLog.count(unhandled)

	l := newServedLayer(t)

	l.send(t, "OPTIONS")
	_, ok := l.tryReceive(t, 500*time.Millisecond)
	assert.False(t, ok, "a client-only layer must send nothing for a request")
	require.Eventually(t, func() bool {
		return packageLog.count(unhandled) == before+1
	}, 5*time.Second, 10*time.Millisecond)
}

// testCtx is the test's root ctx, ended when the test ends: the module's go
// directive predates testing.T.Context.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
