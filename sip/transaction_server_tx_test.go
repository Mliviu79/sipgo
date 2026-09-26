package sip

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/emiago/sipgo/fakes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerTransactionFSM(t *testing.T) {
	// SetTimers(1*time.Millisecond, 1*time.Millisecond, 1*time.Millisecond)
	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "UDP", "127.0.0.2:5060")

	incoming := bytes.NewBuffer([]byte{})
	outgoing := bytes.NewBuffer([]byte{})

	t.Run("PassUpResponse", func(t *testing.T) {
		conn := &UDPConnection{
			PacketConn: &fakes.UDPConn{
				Reader:  incoming,
				Writers: map[string]io.Writer{"127.0.0.2:5060": outgoing},
			},
		}
		tx := NewServerTx("123", req, conn, slog.Default())
		err := tx.Init()
		require.NoError(t, err)

		err = tx.Receive(req)
		require.NoError(t, err)

		require.NoError(t, tx.Err())
		select {
		case <-tx.Done():
			t.Error("Transaction should not terminate")
		default:
		}
	})

	t.Run("OutOfOrderResponse", func(t *testing.T) {
		conn := &UDPConnection{
			PacketConn: &fakes.UDPConn{
				Reader:  incoming,
				Writers: map[string]io.Writer{"127.0.0.2:5060": outgoing},
			},
		}
		tx := NewServerTx("123", req, conn, slog.Default())
		err := tx.Init()
		require.NoError(t, err)

		// We received Cancel while dealing with resposn

		res100 := NewResponseFromRequest(req, StatusTrying, "Trying", nil)
		res200 := NewResponseFromRequest(req, StatusOK, "OK", nil)

		require.NoError(t, tx.Respond(res200))
		require.NoError(t, tx.Respond(res100))
		require.NoError(t, tx.Respond(res100))

		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateAccepted))
	})

}

func TestServerTransactionNonInviteFSM(t *testing.T) {
	// SetTimers(1*time.Millisecond, 1*time.Millisecond, 1*time.Millisecond)

	incoming := bytes.NewBuffer([]byte{})
	outgoing := bytes.NewBuffer([]byte{})

	conn := &UDPConnection{
		PacketConn: &fakes.UDPConn{
			Reader:  incoming,
			Writers: map[string]io.Writer{"127.0.0.1:5060": outgoing},
		},
	}

	t.Run("UDP", func(t *testing.T) {
		req := testCreateRequest(t, "OPTIONS", "sip:example.com", "UDP", "127.0.0.1:5060")
		tx := NewServerTx("123", req, conn, slog.Default())
		err := tx.Init()
		require.NoError(t, err)

		err = tx.Receive(req)
		require.NoError(t, err)
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateTrying))

		// passing 200 response
		err = tx.Respond(NewResponseFromRequest(req, 200, "OK", nil))
		require.NoError(t, err)
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateCompleted))

		// Timer j must be started. Its callback clears tx.timer_j under tx.mu
		// when it fires, so the test reads it under the same lock.
		tx.mu.Lock()
		timerJ := tx.timer_j
		tx.mu.Unlock()
		require.NotNil(t, timerJ)
	})

	t.Run("TCP", func(t *testing.T) {
		req := testCreateRequest(t, "OPTIONS", "sip:example.com", "TCP", "127.0.0.1:5060")
		tx := NewServerTx("123", req, conn, slog.Default())
		err := tx.Init()
		require.NoError(t, err)

		err = tx.Receive(req)
		require.NoError(t, err)
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateTrying))

		// passing 200 response
		err = tx.Respond(NewResponseFromRequest(req, 200, "OK", nil))
		require.NoError(t, err)

		// timer J should be zero, so Completed ends as soon as the timer
		// fires and the transaction terminates without any further input.
		require.Zero(t, tx.timer_j_time)
		select {
		case <-tx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("transaction did not terminate on timer J")
		}
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.stateTerminated))
		require.ErrorIs(t, tx.Err(), ErrTransactionTerminated)
	})
}

// TestServerTransactionRespondReliableFinal sends the final response to a
// non-INVITE request over a reliable transport. Timer J is zero there, so the
// transaction terminates as soon as the response is sent, and Respond must
// still report the send as done. The termination runs on the timer's own
// goroutine and only rarely comes first, so the exchange is repeated.
func TestServerTransactionRespondReliableFinal(t *testing.T) {
	conn := &UDPConnection{
		PacketConn: &fakes.UDPConn{
			Writers: map[string]io.Writer{"127.0.0.1:5060": io.Discard},
		},
	}
	for i := range 2000 {
		req := testCreateRequest(t, "OPTIONS", "sip:example.com", "TCP", "127.0.0.1:5060")
		tx := NewServerTx("123", req, conn, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, tx.Init())

		require.NoError(t, tx.Respond(NewResponseFromRequest(req, 200, "OK", nil)), "exchange %d", i)
		select {
		case <-tx.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("exchange %d: transaction did not terminate on timer J", i)
		}
	}
}

func TestServerTransactionFSMInvite(t *testing.T) {
	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "udp", "127.0.0.2:5060")

	incoming := bytes.NewBuffer([]byte{})
	outgoing := bytes.NewBuffer([]byte{})
	t.Run("InviteCancel", func(t *testing.T) {
		conn := &UDPConnection{
			PacketConn: &fakes.UDPConn{
				Reader:  incoming,
				Writers: map[string]io.Writer{"127.0.0.2:5060": outgoing},
			},
		}
		tx := NewServerTx("123", req, conn, slog.Default())
		err := tx.Init()
		require.NoError(t, err)
		// Timer I is set on the transaction itself, so no package-level timer
		// is changed.
		tx.mu.Lock()
		tx.timer_i_time = 10 * time.Millisecond
		tx.mu.Unlock()

		// We received Cancel while dealing with resposn
		res100 := NewResponseFromRequest(req, StatusTrying, "Trying", nil)
		require.NoError(t, tx.Respond(res100))

		// Cancel will play
		cancelReq := NewRequest(CANCEL, req.Recipient)
		cancelReq.AppendHeader(HeaderClone(req.Via())) // Cancel request must match invite TOP via and only have that Via
		cancelReq.AppendHeader(HeaderClone(req.From()))
		cancelReq.AppendHeader(HeaderClone(req.To()))
		cancelReq.AppendHeader(HeaderClone(req.CallID()))

		require.NoError(t, tx.Receive(cancelReq))
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateCompleted))

		ack := NewRequest(ACK, req.Recipient)
		ack.AppendHeader(HeaderClone(req.Via())) // Cancel request must match invite TOP via and only have that Via
		ack.AppendHeader(HeaderClone(req.From()))
		ack.AppendHeader(HeaderClone(req.To()))
		ack.AppendHeader(HeaderClone(req.CallID()))
		require.NoError(t, tx.Receive(ack))

		select {
		case <-tx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("transaction did not terminate on Timer I")
		}
		require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateTerminated))
	})
}

func TestServerTransactionAckSendMissingCallID(t *testing.T) {
	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "udp", "127.0.0.2:5060")
	tx := NewServerTx("123", req, nil, slog.Default())
	ack := NewRequest(ACK, req.Recipient)

	close(tx.done)

	require.NotPanics(t, func() {
		tx.ackSend(ack, false)
	})
}

// newTestInviteServerTx builds an initialised INVITE server transaction over a
// fake UDP connection, logging to the returned capture. Timer I is set on the
// transaction itself, so no package-level timer is changed.
func newTestInviteServerTx(t *testing.T, timerI time.Duration) (*ServerTx, *Request, *logCapture) {
	t.Helper()
	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "udp", "127.0.0.2:5060")
	conn := &UDPConnection{
		PacketConn: &fakes.UDPConn{
			Reader:  bytes.NewBuffer([]byte{}),
			Writers: map[string]io.Writer{"127.0.0.2:5060": bytes.NewBuffer([]byte{})},
		},
	}
	capture := &logCapture{}
	tx := NewServerTx("123", req, conn, slog.New(capture))
	require.NoError(t, tx.Init())
	tx.mu.Lock()
	tx.timer_i_time = timerI
	tx.mu.Unlock()
	t.Cleanup(tx.Terminate)
	return tx, req, capture
}

// newTestAck builds the ACK a UAC sends on the branch of req.
func newTestAck(req *Request) *Request {
	ack := NewRequest(ACK, req.Recipient)
	ack.AppendHeader(HeaderClone(req.Via()))
	ack.AppendHeader(HeaderClone(req.From()))
	ack.AppendHeader(HeaderClone(req.To()))
	ack.AppendHeader(HeaderClone(req.CallID()))
	return ack
}

// TestServerTransactionAbsorbsNonSuccessAck proves that the ACK for a non-2xx
// final response, which the transaction consumes itself (RFC 3261 17.2.1),
// moves it to Confirmed and is never recorded as missed, including after Timer
// I ends the transaction with nobody reading Acks().
func TestServerTransactionAbsorbsNonSuccessAck(t *testing.T) {
	tests := []struct {
		name     string
		complete func(t *testing.T, tx *ServerTx, req *Request)
	}{
		{
			name: "486 answer",
			complete: func(t *testing.T, tx *ServerTx, req *Request) {
				require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusBusyHere, "Busy Here", nil)))
			},
		},
		{
			name: "487 after CANCEL",
			complete: func(t *testing.T, tx *ServerTx, req *Request) {
				require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusTrying, "Trying", nil)))
				cancelReq := NewRequest(CANCEL, req.Recipient)
				cancelReq.AppendHeader(HeaderClone(req.Via()))
				cancelReq.AppendHeader(HeaderClone(req.From()))
				cancelReq.AppendHeader(HeaderClone(req.To()))
				cancelReq.AppendHeader(HeaderClone(req.CallID()))
				require.NoError(t, tx.Receive(cancelReq))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tx, req, capture := newTestInviteServerTx(t, 200*time.Millisecond)
			tc.complete(t, tx, req)
			require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateCompleted))

			require.NoError(t, tx.Receive(newTestAck(req)))
			require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateConfirmed))

			select {
			case <-tx.Done():
			case <-time.After(2 * time.Second):
				t.Fatal("transaction did not end on Timer I")
			}
			assert.Never(t, func() bool { return capture.count("ACK missed") > 0 }, 300*time.Millisecond, 10*time.Millisecond,
				"the absorbed ACK was recorded as missed")
		})
	}
}

// TestServerTransactionOffersAbsorbedAckToWaitingReader proves that the ACK for
// a non-2xx final response still reaches a TU that starts reading Acks() after
// the ACK arrived, which DialogServerSession.WriteResponse relies on.
func TestServerTransactionOffersAbsorbedAckToWaitingReader(t *testing.T) {
	tx, req, _ := newTestInviteServerTx(t, 10*time.Second)
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusBusyHere, "Busy Here", nil)))

	ack := newTestAck(req)
	require.NoError(t, tx.Receive(ack))

	select {
	case got := <-tx.Acks():
		require.Same(t, ack, got)
	case <-time.After(time.Second):
		t.Fatal("the ACK was not offered on Acks()")
	}
}

// TestServerTransactionPassesUpSuccessAck proves that an ACK received in the
// Accepted state is passed up to a reader of Acks() (RFC 6026 7.1).
func TestServerTransactionPassesUpSuccessAck(t *testing.T) {
	tx, req, _ := newTestInviteServerTx(t, 10*time.Second)
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))
	require.NoError(t, compareFunctions(tx.currentFsmState(), tx.inviteStateAccepted))

	ack := newTestAck(req)
	require.NoError(t, tx.Receive(ack))

	select {
	case got := <-tx.Acks():
		require.Same(t, ack, got)
	case <-time.After(time.Second):
		t.Fatal("the ACK was not passed up on Acks()")
	}
}

// TestServerTransactionWarnsWhenSuccessAckUnread proves that an ACK passed up in
// the Accepted state, which the TU is expected to read, is recorded once as
// missed at Warn when the transaction ends before anybody reads it.
func TestServerTransactionWarnsWhenSuccessAckUnread(t *testing.T) {
	tx, req, capture := newTestInviteServerTx(t, 10*time.Second)
	require.NoError(t, tx.Respond(NewResponseFromRequest(req, StatusOK, "OK", nil)))
	require.NoError(t, tx.Receive(newTestAck(req)))

	tx.Terminate()

	require.Eventually(t, func() bool { return capture.count("ACK missed") > 0 }, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, capture.count("ACK missed"))
	r := capture.find(t, "ACK missed")
	assert.Equal(t, slog.LevelWarn, r.Level)
	var txKey string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "tx" {
			txKey = a.Value.String()
		}
		return true
	})
	assert.Equal(t, tx.Key(), txKey)
}

func TestServerTransactionContext(t *testing.T) {
	req, _, _ := testCreateInvite(t, "sip:127.0.0.99:5060", "udp", "127.0.0.2:5060")
	tx := NewServerTx("123", req, nil, slog.Default())
	ctx := ServerTransactionContext(tx)
	tx.Terminate()
	require.Equal(t, context.Canceled, ctx.Err())
	require.Equal(t, ErrTransactionTerminated, tx.Err())
}

func TestServerTransactionReleasesConnRef(t *testing.T) {
	req := testCreateRequest(t, "OPTIONS", "sip:example.com", "UDP", "127.0.0.1:5060")
	conn := &UDPConnection{
		PacketConn: &fakes.UDPConn{
			Reader:  bytes.NewBuffer([]byte{}),
			Writers: map[string]io.Writer{},
		},
	}
	conn.Ref(2) // serverRequestConnection holds a reference before NewServerTx

	tx := NewServerTx("123", req, conn, slog.Default())
	require.NoError(t, tx.Init())

	tx.Terminate()
	<-tx.Done()

	require.Equal(t, 1, conn.Ref(0), "Terminate must release exactly one connection reference")
}
