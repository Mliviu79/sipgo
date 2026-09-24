package sipgo

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/emiago/sipgo/sip"
)

// malformedLogCapture is a slog.Handler retaining every record, so a test can
// assert on the level and the attributes the transaction layer writes when it
// rejects a request it cannot build a transaction for.
type malformedLogCapture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *malformedLogCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *malformedLogCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *malformedLogCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *malformedLogCapture) WithGroup(string) slog.Handler { return h }

// snapshot returns a copy of every record captured so far.
func (h *malformedLogCapture) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

// recordAttrs renders every attribute of a record by key.
func recordAttrs(r slog.Record) map[string]string {
	attrs := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value.String()
		return true
	})
	return attrs
}

// malformedBodyMarker is carried in the rejected request's body, so a test can
// prove no log record copies the payload a remote peer sent.
const malformedBodyMarker = "sipgo-malformed-body-marker"

// TestServerRejectsRequestWithoutCSeq proves, over a real UDP listener, that a
// request with no CSeq is answered 400 with a reason naming the missing header,
// on the first datagram and on its retransmission, and that each datagram is
// recorded once at Debug without the payload rather than at Error.
func TestServerRejectsRequestWithoutCSeq(t *testing.T) {
	tests := []struct {
		name   string
		method sip.RequestMethod
	}{
		{name: "INVITE", method: sip.INVITE},
		{name: "OPTIONS", method: sip.OPTIONS},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			capture := &malformedLogCapture{}
			ua, err := NewUA(WithUserAgentTransactionLayerOptions(
				sip.WithTransactionLayerLogger(slog.New(capture)),
			))
			require.NoError(t, err)

			srv, err := NewServer(ua)
			require.NoError(t, err)
			var handled atomic.Int32
			srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) { handled.Add(1) })
			srv.OnOptions(func(req *sip.Request, tx sip.ServerTransaction) { handled.Add(1) })

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
			clientAddr := clientConn.LocalAddr().String()

			body := "v=0\r\ns=" + malformedBodyMarker + "\r\n"
			raw := []byte(strings.Join([]string{
				string(tc.method) + " sip:server@127.0.0.1:" + strconv.Itoa(serverAddr.Port) + " SIP/2.0",
				"Via: SIP/2.0/UDP " + clientAddr + ";branch=z9hG4bK-nocseq-" + tc.name,
				"From: <sip:client@" + clientAddr + ">;tag=nocseq",
				"To: <sip:server@127.0.0.1:" + strconv.Itoa(serverAddr.Port) + ">",
				"Call-ID: nocseq-" + tc.name,
				"Max-Forwards: 70",
				"Content-Type: application/sdp",
				"Content-Length: " + strconv.Itoa(len(body)),
				"",
				body,
			}, "\r\n"))

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

			// The second write is the retransmission of the first.
			for i := range 2 {
				_, err := clientConn.WriteTo(raw, serverAddr)
				require.NoError(t, err)
				res := receive(t)
				assert.Equal(t, sip.StatusBadRequest, res.StatusCode, "datagram %d", i)
				assert.Equal(t, "Missing CSeq Header Field", res.Reason, "datagram %d", i)
			}
			assert.EqualValues(t, 0, handled.Load(), "a handler ran for a request with no CSeq")

			const rejected = "Rejected malformed request"
			countRejected := func() int {
				n := 0
				for _, r := range capture.snapshot() {
					if r.Message == rejected {
						n++
					}
				}
				return n
			}
			assert.Eventually(t, func() bool { return countRejected() >= 2 }, 2*time.Second, 10*time.Millisecond,
				"expected one %q record per datagram", rejected)

			records := capture.snapshot()
			var rejections int
			for _, r := range records {
				attrs := recordAttrs(r)
				assert.NotEqual(t, slog.LevelError, r.Level, "Error record %q %v", r.Message, attrs)
				for k, v := range attrs {
					assert.NotContains(t, v, malformedBodyMarker, "record %q attr %q carries the payload", r.Message, k)
				}
				if r.Message != rejected {
					continue
				}
				rejections++
				assert.Equal(t, slog.LevelDebug, r.Level)
				assert.Equal(t, string(tc.method), attrs["method"])
				assert.Equal(t, clientAddr, attrs["src"])
				assert.Equal(t, "Missing CSeq Header Field", attrs["reason"])
				assert.Contains(t, attrs["error"], "CSeq")
			}
			assert.Equal(t, 2, rejections)
		})
	}
}
