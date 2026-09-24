package sipgo

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/emiago/sipgo/sip"
)

// handlerPanicSentinel is the value the test handlers panic with, so the
// child's crash output and the captured log record can both be matched on it.
const handlerPanicSentinel = "sipgo test handler panic"

// panicLogCapture is a slog.Handler retaining every record, so the child can
// assert on the record the transaction layer writes for a panicking handler.
type panicLogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *panicLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *panicLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *panicLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *panicLogCapture) WithGroup(string) slog.Handler { return h }

// panicRecord returns the attributes of the "Request handler panicked" record
// logged for the request with this Call-ID, and whether one was captured.
func (h *panicLogCapture) panicRecord(callID string) (slog.Level, map[string]string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message != "Request handler panicked" {
			continue
		}
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		if attrs["callid"] == callID {
			return r.Level, attrs, true
		}
	}
	return 0, nil, false
}

// TestServerRecoversPanickingHandler proves, over a real UDP listener, that a
// handler panic is answered 500 and the server keeps serving. A panic that
// escapes kills the whole test binary, so the scenario runs in a child process
// and the parent asserts on its exit status and output.
func TestServerRecoversPanickingHandler(t *testing.T) {
	if os.Getenv("SIPGO_PANIC_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0],
			"-test.run=^TestServerRecoversPanickingHandler$",
			"-test.count=1",
			"-test.v",
			"-test.timeout=60s",
		)
		cmd.Env = append(os.Environ(), "SIPGO_PANIC_CHILD=1")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "child process failed:\n%s", out)
		require.NotContains(t, string(out), "panic: "+handlerPanicSentinel, "child output:\n%s", out)
		return
	}

	capture := &panicLogCapture{}
	ua, err := NewUA(WithUserAgentTransactionLayerOptions(
		sip.WithTransactionLayerLogger(slog.New(capture)),
	))
	require.NoError(t, err)

	srv, err := NewServer(ua)
	require.NoError(t, err)
	srv.OnInfo(func(req *sip.Request, tx sip.ServerTransaction) {
		panic(handlerPanicSentinel)
	})
	srv.OnOptions(func(req *sip.Request, tx sip.ServerTransaction) {
		if err := tx.Respond(sip.NewResponseFromRequest(req, sip.StatusOK, "OK", nil)); err != nil {
			t.Errorf("respond 200 to OPTIONS: %v", err)
		}
	})

	serverConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	served := make(chan struct{})
	go func() {
		defer close(served)
		_ = srv.ServeUDP(serverConn)
	}()
	t.Cleanup(func() {
		_ = serverConn.Close()
		_ = ua.Close()
		<-served
	})

	clientConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })

	serverAddr := serverConn.LocalAddr().(*net.UDPAddr)
	clientAddr := clientConn.LocalAddr().(*net.UDPAddr)
	serverURI := sip.Uri{User: "server", Host: "127.0.0.1", Port: serverAddr.Port}
	clientURI := sip.Uri{User: "client", Host: "127.0.0.1", Port: clientAddr.Port}

	send := func(t *testing.T, raw []byte) {
		t.Helper()
		_, err := clientConn.WriteTo(raw, serverAddr)
		require.NoError(t, err)
	}
	receive := func(t *testing.T) *sip.Response {
		t.Helper()
		buf := make([]byte, 65535)
		require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(5*time.Second)))
		n, _, err := clientConn.ReadFrom(buf)
		require.NoError(t, err)
		msg, err := sip.ParseMessage(buf[:n])
		require.NoError(t, err)
		res, ok := msg.(*sip.Response)
		require.True(t, ok, "expected a SIP response, got %q", buf[:n])
		return res
	}
	assertServerError := func(t *testing.T, req *sip.Request, res *sip.Response) {
		t.Helper()
		assert.Equal(t, sip.StatusInternalServerError, res.StatusCode)
		assert.Equal(t, "Server Internal Error", res.Reason)
		require.NotNil(t, res.CallID())
		assert.Equal(t, req.CallID().Value(), res.CallID().Value())
	}

	info := createSimpleRequest(sip.INFO, clientURI, serverURI, "UDP")
	send(t, []byte(info.String()))
	assertServerError(t, info, receive(t))

	level, attrs, ok := capture.panicRecord(info.CallID().Value())
	require.True(t, ok, "no 'Request handler panicked' record for the INFO")
	assert.Equal(t, slog.LevelError, level)
	assert.Equal(t, "INFO", attrs["method"])
	assert.Contains(t, attrs["panic"], handlerPanicSentinel)
	assert.NotEmpty(t, attrs["stack"])
	assert.Contains(t, attrs["stack"], "TestServerRecoversPanickingHandler")
	assert.NotEmpty(t, attrs["tx"])

	options := createSimpleRequest(sip.OPTIONS, clientURI, serverURI, "UDP")
	send(t, []byte(options.String()))
	res := receive(t)
	assert.Equal(t, sip.StatusOK, res.StatusCode)

	secondInfo := createSimpleRequest(sip.INFO, clientURI, serverURI, "UDP")
	send(t, []byte(secondInfo.String()))
	assertServerError(t, secondInfo, receive(t))
}
