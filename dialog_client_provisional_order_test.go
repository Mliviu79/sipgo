//go:build go1.25

package sipgo

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// provisionalsThenAnswer are the reason phrases of the responses a peer sends
// back to back to an INVITE: two provisionals, then the 2xx. The phrases tell
// the two provisionals apart.
var provisionalsThenAnswer = []struct {
	code   int
	reason string
}{
	{code: sip.StatusSessionInProgress, reason: "First Progress"},
	{code: sip.StatusSessionInProgress, reason: "Second Progress"},
	{code: sip.StatusOK, reason: "OK"},
}

// reasonsSeen returns the AnswerOptions whose OnResponse records the reason
// phrase of every response it is called with, and the function that returns
// them once the wait has returned.
func reasonsSeen() (AnswerOptions, func() []string) {
	var seen []string
	opts := AnswerOptions{OnResponse: func(res *sip.Response) error {
		seen = append(seen, res.Reason)
		return nil
	}}
	return opts, func() []string { return seen }
}

// wantReasons are the reason phrases OnResponse must see: every response the
// peer sent, in the order it sent them (RFC 3261 sections 13.2.2.1 and
// 17.1.1.2).
func wantReasons() []string {
	var want []string
	for _, r := range provisionalsThenAnswer {
		want = append(want, r.reason)
	}
	return want
}

// TestDialogClientProvisionalsInOrder has the transaction of the INVITE
// receive two provisionals and the 2xx back to back, in order, while
// WaitAnswer waits. OnResponse sees each of them, in order.
func TestDialogClientProvisionalsInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, d := newScriptedDialog(t)
		opts, seen := reasonsSeen()
		waited := make(chan error, 1)
		go func() { waited <- d.WaitAnswer(t.Context(), opts) }()

		invite := p.next(t, sip.INVITE)
		go func() {
			for _, r := range provisionalsThenAnswer {
				invite.w.Receive(p.response(invite, r.code, r.reason))
			}
		}()
		require.NoError(t, <-waited)
		require.Equal(t, wantReasons(), seen(), "the responses OnResponse saw")
	})
}

// TestDialogClientProvisionalsInOrderOverUDP is TestDialogClientProvisionalsInOrder
// over a socket: the peer writes the two provisionals and the 2xx back to
// back, and the transport and transaction layers carry them to the wait.
func TestDialogClientProvisionalsInOrderOverUDP(t *testing.T) {
	u := newRawUAS(t)
	d := newRawClientDialog(t, u)
	opts, seen := reasonsSeen()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	waited := make(chan error, 1)
	go func() { waited <- d.WaitAnswer(ctx, opts) }()

	invite := u.expect(t, sip.INVITE)
	for _, r := range provisionalsThenAnswer {
		res := sip.NewResponseFromRequest(invite.req, r.code, r.reason, nil)
		res.To().Params.Add("tag", u.toTag)
		res.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: u.port}})
		u.send(t, res, invite.src)
	}
	require.NoError(t, <-waited)
	require.Equal(t, wantReasons(), seen(), "the responses OnResponse saw")
}
