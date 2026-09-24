package sipgo

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/emiago/sipgo/sip"
)

// TestServerAbsorbsAckForNonSuccessAnswer proves, over a real UDP listener, that
// the ACK for a 486 a handler sent through tx.Respond, without reading Acks(),
// stops the answer's retransmission and is never recorded as missed, including
// after Timer I ends the transaction.
func TestServerAbsorbsAckForNonSuccessAnswer(t *testing.T) {
	capture := &malformedLogCapture{}
	ua, err := NewUA(WithUserAgentTransactionLayerOptions(
		sip.WithTransactionLayerLogger(slog.New(capture)),
	))
	require.NoError(t, err)

	srv, err := NewServer(ua)
	require.NoError(t, err)
	handled := make(chan *sip.ServerTx, 1)
	srv.OnInvite(func(req *sip.Request, tx sip.ServerTransaction) {
		_ = tx.Respond(sip.NewResponseFromRequest(req, sip.StatusBusyHere, "Busy Here", nil))
		handled <- tx.(*sip.ServerTx)
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
	clientAddr := clientConn.LocalAddr().String()
	requestURI := "sip:server@127.0.0.1:" + strconv.Itoa(serverAddr.Port)
	via := "Via: SIP/2.0/UDP " + clientAddr + ";branch=z9hG4bK-absorbed-ack"
	from := "From: <sip:client@" + clientAddr + ">;tag=absorbed"
	callID := "Call-ID: absorbed-ack"

	invite := []byte(strings.Join([]string{
		"INVITE " + requestURI + " SIP/2.0",
		via,
		from,
		"To: <" + requestURI + ">",
		callID,
		"CSeq: 1 INVITE",
		"Max-Forwards: 70",
		"Content-Length: 0",
		"",
		"",
	}, "\r\n"))
	_, err = clientConn.WriteTo(invite, serverAddr)
	require.NoError(t, err)

	// readFinal returns the next final response, skipping provisional ones, or
	// nil when nothing arrives before the deadline.
	readFinal := func(t *testing.T, deadline time.Time) *sip.Response {
		t.Helper()
		buf := make([]byte, 65535)
		require.NoError(t, clientConn.SetReadDeadline(deadline))
		for {
			n, _, err := clientConn.ReadFrom(buf)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return nil
			}
			require.NoError(t, err)
			msg, err := sip.ParseMessage(buf[:n])
			require.NoError(t, err)
			res, ok := msg.(*sip.Response)
			require.True(t, ok, "expected a SIP response, got %q", buf[:n])
			if !res.IsProvisional() {
				return res
			}
		}
	}

	res := readFinal(t, time.Now().Add(5*time.Second))
	require.NotNil(t, res, "no final response to the INVITE")
	require.Equal(t, sip.StatusBusyHere, res.StatusCode)

	var tx *sip.ServerTx
	select {
	case tx = <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the INVITE handler did not run")
	}

	ack := []byte(strings.Join([]string{
		"ACK " + requestURI + " SIP/2.0",
		via,
		from,
		"To: " + res.To().Value(),
		callID,
		"CSeq: 1 ACK",
		"Max-Forwards: 70",
		"Content-Length: 0",
		"",
		"",
	}, "\r\n"))
	_, err = clientConn.WriteTo(ack, serverAddr)
	require.NoError(t, err)

	// Timer G is 500 ms, so a transaction that had not absorbed the ACK would
	// resend the 486 within this window.
	assert.Nil(t, readFinal(t, time.Now().Add(1500*time.Millisecond)), "the 486 was resent after the ACK")

	select {
	case <-tx.Done():
	case <-time.After(sip.Timer_I + 3*time.Second):
		t.Fatal("the transaction did not end on Timer I")
	}

	missed := func() []slog.Record {
		var out []slog.Record
		for _, r := range capture.snapshot() {
			if r.Message == "ACK missed" {
				out = append(out, r)
			}
		}
		return out
	}
	if !assert.Never(t, func() bool { return len(missed()) > 0 }, 500*time.Millisecond, 10*time.Millisecond,
		"the absorbed ACK was recorded as missed on transaction %s", tx.Key()) {
		for _, r := range missed() {
			t.Logf("captured %q at %s: %v", r.Message, r.Level, recordAttrs(r))
		}
	}
}
