//go:build go1.25

package sipgo

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/emiago/sipgo/siptest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// errGaveUp is the cause a test ends the wait for the INVITE's answer
	// with.
	errGaveUp = errors.New("the caller gave the INVITE up")
	// errOwnerStopped is the cause a test ends the context of Cancel with, as
	// the owner of the call the INVITE attempts does when it stops.
	errOwnerStopped = errors.New("the owner of the call stopped")
)

// sentRequest is a request the dialog sent, with the responder of the
// transaction it was sent on.
type sentRequest struct {
	req *sip.Request
	w   *siptest.ClientTxResponder
}

// scriptedPeer is the far end of a dialog whose client sends on siptest
// transactions, which use no socket, so it runs inside a synctest bubble. It
// holds each request the dialog sends, and answers only what the test tells
// it to.
type scriptedPeer struct {
	sent chan sentRequest
}

// newScriptedDialog sends an INVITE to a scriptedPeer, with the given headers
// added to it, and returns both. It ends the INVITE transaction with the
// test, so a response the dialog left unread does not hold its delivery past
// the bubble.
func newScriptedDialog(t *testing.T, headers ...sip.Header) (*scriptedPeer, *DialogClientSession) {
	t.Helper()
	p := &scriptedPeer{sent: make(chan sentRequest, 16)}
	client := testClientResponder(t, func(req *sip.Request, w *siptest.ClientTxResponder) {
		p.sent <- sentRequest{req: req, w: w}
	})
	dua := DialogUA{
		Client:     client,
		ContactHDR: sip.ContactHeader{Address: sip.Uri{User: "uac", Host: "127.0.0.1", Port: 5060}},
	}
	d, err := dua.Invite(t.Context(), sip.Uri{User: "uas", Host: "localhost"}, nil, headers...)
	require.NoError(t, err)
	t.Cleanup(func() {
		d.inviteTx.Terminate()
		_ = d.Close()
	})
	return p, d
}

// next returns the request the dialog has sent by the time every goroutine
// has settled. It fails the test when there is none, or one of another method.
func (p *scriptedPeer) next(t *testing.T, method sip.RequestMethod) sentRequest {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-p.sent:
		require.Equal(t, method, r.req.Method, "the request the dialog sent")
		return r
	default:
		t.Fatalf("the dialog sent no %s", method)
		return sentRequest{}
	}
}

// sentNothing fails the test when, every goroutine settled, the dialog has
// sent a request the test did not take with next.
func (p *scriptedPeer) sentNothing(t *testing.T) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-p.sent:
		t.Fatalf("the dialog sent a %s", r.req.Method)
	default:
	}
}

// response returns the response of the given status the peer has for r,
// which the test sends with deliver. A response to the INVITE carries the
// peer's To tag and Contact, so its 2xx sets up the dialog.
func (*scriptedPeer) response(r sentRequest, code int, reason string) *sip.Response {
	res := sip.NewResponseFromRequest(r.req, code, reason, nil)
	if r.req.IsInvite() {
		res.To().Params.Add("tag", "uas-tag")
		res.AppendHeader(&sip.ContactHeader{Address: sip.Uri{User: "uas", Host: "127.0.0.1", Port: 5090}})
	}
	return res
}

// deliver has the transaction of r receive res and returns once every
// goroutine has settled. The transaction hands the response to whoever waits
// on it, so the delivery runs on a goroutine of its own, which ends when the
// response is read or the transaction ends.
func (*scriptedPeer) deliver(r sentRequest, res *sip.Response) {
	go r.w.Receive(res)
	synctest.Wait()
}

// answer has the transaction of r receive a response of the given status, as
// response builds it, and returns it once every goroutine has settled.
func (p *scriptedPeer) answer(r sentRequest, code int, reason string) *sip.Response {
	res := p.response(r, code, reason)
	p.deliver(r, res)
	return res
}

// inviteEnded reports whether the dialog's INVITE transaction has ended.
func inviteEnded(d *DialogClientSession) bool {
	select {
	case <-d.inviteTx.Done():
		return true
	default:
		return false
	}
}

// leavePending has the dialog wait for its INVITE's answer with
// WaitAnswerLeavePending, on a context the test then ends with errGaveUp, and
// returns the INVITE the peer holds. With ringing the peer sends a 180 first,
// which the wait reads before its context ends.
func leavePending(t *testing.T, p *scriptedPeer, d *DialogClientSession, ringing bool) sentRequest {
	t.Helper()
	ctx, giveUp := context.WithCancelCause(t.Context())
	defer giveUp(nil)
	waited := make(chan error, 1)
	go func() { waited <- d.WaitAnswerLeavePending(ctx, AnswerOptions{}) }()

	invite := p.next(t, sip.INVITE)
	if ringing {
		p.answer(invite, sip.StatusRinging, "Ringing")
	}
	giveUp(errGaveUp)
	err := <-waited
	require.ErrorIs(t, err, ErrInvitePending)
	require.ErrorIs(t, err, errGaveUp, "the cause of the context's end is wrapped")
	return invite
}

// cancelling starts Cancel on ctx and returns the channel its result arrives
// on.
func cancelling(ctx context.Context, d *DialogClientSession) <-chan error {
	cancelled := make(chan error, 1)
	go func() { cancelled <- d.Cancel(ctx) }()
	return cancelled
}

// TestDialogClientWaitAnswerLeavePending has the context of the wait for the
// INVITE's answer end before a final response. The wait returns
// ErrInvitePending with the context's cause, sends no CANCEL and leaves the
// INVITE transaction running; Cancel then withdraws the INVITE on a context
// of its own: it sends the CANCEL only once a provisional has come (RFC 3261
// section 9.1), and returns nil on the 487 that ends the INVITE.
func TestDialogClientWaitAnswerLeavePending(t *testing.T) {
	tests := []struct {
		name string
		// ringing has the peer send a 180 before the wait's context ends.
		// Without it the context ends before any response.
		ringing bool
	}{
		{name: "BeforeAnyProvisional"},
		{name: "AfterAProvisional", ringing: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				invite := leavePending(t, p, d, tc.ringing)

				p.sentNothing(t)
				assert.False(t, inviteEnded(d), "the INVITE transaction is left running")
				assert.Equal(t, tc.ringing, d.InviteResponse != nil, "a response was read")

				cancelled := cancelling(t.Context(), d)
				if !tc.ringing {
					p.sentNothing(t)
					p.answer(invite, sip.StatusRinging, "Ringing")
				}
				cancelReq := p.next(t, sip.CANCEL)
				p.answer(cancelReq, sip.StatusOK, "OK")
				p.answer(invite, sip.StatusRequestTerminated, "Request Terminated")

				require.NoError(t, <-cancelled)
				require.NotNil(t, d.InviteResponse)
				assert.Equal(t, sip.StatusRequestTerminated, d.InviteResponse.StatusCode)
				assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientWaitAnswerLeavePendingByDeadline has the context of the
// wait end by its deadline: the error wraps the deadline's error with
// ErrInvitePending, at the deadline, and nothing is sent.
func TestDialogClientWaitAnswerLeavePendingByDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, d := newScriptedDialog(t)
		p.next(t, sip.INVITE)

		const patience = 3 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), patience)
		defer cancel()
		start := time.Now()
		err := d.WaitAnswerLeavePending(ctx, AnswerOptions{})

		require.ErrorIs(t, err, ErrInvitePending)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, patience, time.Since(start))
		assert.False(t, inviteEnded(d), "the INVITE transaction is left running")
		p.sentNothing(t)
	})
}

// TestDialogClientCancelAnswered has a 2xx answer the INVITE that Cancel
// withdraws: it crosses the CANCEL, follows the CANCEL's own answer, or comes
// before a provisional let a CANCEL be sent (RFC 3261 section 9.1). Every 2xx
// is to be acknowledged (section 13.2.2.4), so Cancel returns
// ErrAnsweredAfterCancel with the dialog established and its INVITE
// transaction kept. Cancel is not given the context that made the caller give
// up, so the error carries none.
func TestDialogClientCancelAnswered(t *testing.T) {
	tests := []struct {
		name string
		// ringing has the peer send a 180 before Cancel, so a CANCEL goes out.
		ringing bool
		// cancelStatus is the peer's answer to the CANCEL.
		cancelStatus int
		cancelReason string
		// crossing has the peer send its 2xx before it answers the CANCEL,
		// which no longer matches the answered INVITE (RFC 3261 section 9.2).
		crossing bool
	}{
		{name: "TheAnswerCrossesTheCancel", ringing: true, cancelStatus: sip.StatusCallTransactionDoesNotExists, cancelReason: "Call/Transaction Does Not Exist", crossing: true},
		{name: "TheCancelIsAnsweredFirst", ringing: true, cancelStatus: sip.StatusOK, cancelReason: "OK"},
		{name: "BeforeAnyProvisional"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				invite := leavePending(t, p, d, tc.ringing)
				cancelled := cancelling(t.Context(), d)

				var final *sip.Response
				switch {
				case !tc.ringing:
					p.sentNothing(t)
					final = p.answer(invite, sip.StatusOK, "OK")
				case tc.crossing:
					cancelReq := p.next(t, sip.CANCEL)
					final = p.answer(invite, sip.StatusOK, "OK")
					p.answer(cancelReq, tc.cancelStatus, tc.cancelReason)
				default:
					cancelReq := p.next(t, sip.CANCEL)
					p.answer(cancelReq, tc.cancelStatus, tc.cancelReason)
					final = p.answer(invite, sip.StatusOK, "OK")
				}

				err := <-cancelled
				var answer *ErrAnsweredAfterCancel
				require.ErrorAs(t, err, &answer)
				assert.Same(t, final, answer.Res)
				require.NoError(t, answer.Err)
				assert.Equal(t, "INVITE answered 200 OK after it was cancelled", answer.Error())
				require.NoError(t, errors.Unwrap(err))

				assert.Equal(t, sip.DialogStateEstablished, d.LoadState())
				wantID, idErr := sip.DialogIDFromResponse(final)
				require.NoError(t, idErr)
				assert.Equal(t, wantID, d.ID)
				assert.Same(t, final, d.InviteResponse)
				assert.False(t, inviteEnded(d), "the INVITE transaction is kept for the ACK")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientCancelRefusedBeforeProvisional has a non-2xx final answer
// the INVITE while Cancel waits for the provisional that would let it send a
// CANCEL: the peer refused the INVITE and nothing was withdrawn, so Cancel
// returns the refusal as ErrDialogResponse and sends no CANCEL.
func TestDialogClientCancelRefusedBeforeProvisional(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, d := newScriptedDialog(t)
		invite := leavePending(t, p, d, false)
		cancelled := cancelling(t.Context(), d)
		p.sentNothing(t)
		final := p.answer(invite, sip.StatusBusyHere, "Busy Here")

		err := <-cancelled
		var refusal *ErrDialogResponse
		require.ErrorAs(t, err, &refusal)
		assert.Same(t, final, refusal.Res)
		assert.Same(t, final, d.InviteResponse)
		assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
		p.sentNothing(t)
	})
}

// TestDialogClientCancelTimers has the peer stay silent under each wait of
// Cancel that a timer of RFC 3261 bounds. With no final for the INVITE within
// 64*T1 of the CANCEL's answer the INVITE is taken as cancelled (section
// 9.1); a CANCEL that is not answered ends on its transaction's Timer F; an
// INVITE with no response at all ends on its own transaction's Timer B, under
// the wait for a provisional. The INVITE transaction is terminated each time.
func TestDialogClientCancelTimers(t *testing.T) {
	tests := []struct {
		name string
		// ringing has the peer send a 180 before Cancel, so a CANCEL goes out.
		ringing bool
		// cancelAnswered has the peer answer the CANCEL 200.
		cancelAnswered bool
		// wantWait is the time Cancel takes, from the INVITE.
		wantWait time.Duration
		// wantErrs holds the errors Cancel may return, one of which it must;
		// empty for a nil result. Timer F and the bound of the CANCEL's wait
		// expire at the same instant, and either may be read first.
		wantErrs []error
	}{
		{name: "NoFinalWithin64T1", ringing: true, cancelAnswered: true, wantWait: 64 * sip.T1},
		{name: "TheCancelIsNotAnswered", ringing: true, wantWait: sip.Timer_F, wantErrs: []error{sip.ErrTransactionTimeout, context.DeadlineExceeded}},
		{name: "TheInviteIsNotAnswered", wantWait: sip.Timer_B, wantErrs: []error{sip.ErrTransactionTimeout}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				p, d := newScriptedDialog(t)
				leavePending(t, p, d, tc.ringing)
				cancelled := cancelling(t.Context(), d)
				if tc.ringing {
					cancelReq := p.next(t, sip.CANCEL)
					if tc.cancelAnswered {
						p.answer(cancelReq, sip.StatusOK, "OK")
					}
				}

				err := <-cancelled
				assert.Equal(t, tc.wantWait, time.Since(start))
				if len(tc.wantErrs) == 0 {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
					matched := false
					for _, want := range tc.wantErrs {
						matched = matched || errors.Is(err, want)
					}
					assert.True(t, matched, "Cancel returned %v, want one of %v", err, tc.wantErrs)
				}
				var answer *ErrAnsweredAfterCancel
				assert.False(t, errors.As(err, &answer), "no 2xx answered the INVITE")
				assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientCancelContextEnds has the context of Cancel end before the
// INVITE does, under each of its waits: for a provisional, for the CANCEL's
// answer, and for the INVITE's final. Cancel returns at that end, with the
// context's cause, and terminates the INVITE transaction.
func TestDialogClientCancelContextEnds(t *testing.T) {
	tests := []struct {
		name string
		// started has Cancel called before its context ends.
		started bool
		// ringing has the peer send a 180 before Cancel, so a CANCEL goes out.
		ringing bool
		// cancelAnswered has the peer answer the CANCEL 200, so Cancel waits
		// for the INVITE's final.
		cancelAnswered bool
	}{
		{name: "BeforeItStarts", ringing: true},
		{name: "WaitingForAProvisional", started: true},
		{name: "WaitingForTheCancelsAnswer", started: true, ringing: true},
		{name: "WaitingForTheFinal", started: true, ringing: true, cancelAnswered: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				leavePending(t, p, d, tc.ringing)
				ctx, stop := context.WithCancelCause(t.Context())
				defer stop(nil)
				if !tc.started {
					stop(errOwnerStopped)
				}
				start := time.Now()
				cancelled := cancelling(ctx, d)
				if tc.started && tc.ringing {
					cancelReq := p.next(t, sip.CANCEL)
					if tc.cancelAnswered {
						p.answer(cancelReq, sip.StatusOK, "OK")
					}
				}
				p.sentNothing(t)
				stop(errOwnerStopped)

				err := <-cancelled
				require.ErrorIs(t, err, errOwnerStopped)
				assert.Zero(t, time.Since(start), "Cancel ends with its context, ahead of the timers")
				var answer *ErrAnsweredAfterCancel
				assert.False(t, errors.As(err, &answer), "no 2xx answered the INVITE")
				assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientCancelAfterFinal calls Cancel for an INVITE that already
// has its final response, which a CANCEL has no effect on (RFC 3261 section
// 9.1): it sends nothing and returns what the final means, the dialog to
// acknowledge and end for a 2xx, the refusal for any other.
func TestDialogClientCancelAfterFinal(t *testing.T) {
	t.Run("Answered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			final := p.answer(p.next(t, sip.INVITE), sip.StatusOK, "OK")
			require.NoError(t, d.WaitAnswerLeavePending(t.Context(), AnswerOptions{}))

			err := d.Cancel(t.Context())
			var answer *ErrAnsweredAfterCancel
			require.ErrorAs(t, err, &answer)
			assert.Same(t, final, answer.Res)
			assert.Equal(t, sip.DialogStateEstablished, d.LoadState())
			assert.False(t, inviteEnded(d), "the INVITE transaction is kept for the ACK")
			p.sentNothing(t)
		})
	})

	t.Run("Refused", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			final := p.answer(p.next(t, sip.INVITE), sip.StatusBusyHere, "Busy Here")
			var refusal *ErrDialogResponse
			require.ErrorAs(t, d.WaitAnswerLeavePending(t.Context(), AnswerOptions{}), &refusal)

			err := d.Cancel(t.Context())
			refusal = nil
			require.ErrorAs(t, err, &refusal)
			assert.Same(t, final, refusal.Res)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})
}

// challengeAnswer holds the credentials a test answers a digest challenge
// with.
var challengeAnswer = AnswerOptions{Username: "user", Password: "secret"}

// challengeTo returns the digest challenge the peer has for the INVITE r,
// which the test sends with deliver.
func (*scriptedPeer) challengeTo(r sentRequest) *sip.Response {
	challenge := sip.NewResponseFromRequest(r.req, sip.StatusUnauthorized, "Unauthorized", nil)
	challenge.AppendHeader(sip.NewHeader("WWW-Authenticate", `Digest realm="test", nonce="662d65a084b88c6d2a745a9de086fa91", algorithm=MD5`))
	return challenge
}

// challenge has the peer answer the INVITE the dialog sent with a digest
// challenge, and returns the challenge and the INVITE the dialog then sends
// again with credentials, on a new transaction. The dialog's wait for the
// answer is running with challengeAnswer.
func (p *scriptedPeer) challenge(t *testing.T) (*sip.Response, sentRequest) {
	t.Helper()
	first := p.next(t, sip.INVITE)
	challenge := p.challengeTo(first)
	go first.w.Receive(challenge)
	resent := p.next(t, sip.INVITE)
	require.NotNil(t, resent.req.GetHeader("Authorization"), "the INVITE is sent again with credentials")
	return challenge, resent
}

// leaveChallengedPending is leavePending for an INVITE the peer challenges:
// the dialog sends it again with credentials, and the wait's context ends
// while that INVITE is pending. It returns the INVITE sent again. With ringing
// the peer answers it with a 180 first, which the wait reads before its
// context ends; without it the challenge is the last response read, and it is
// the final of the INVITE before.
func leaveChallengedPending(t *testing.T, p *scriptedPeer, d *DialogClientSession, ringing bool) sentRequest {
	t.Helper()
	ctx, giveUp := context.WithCancelCause(t.Context())
	defer giveUp(nil)
	waited := make(chan error, 1)
	go func() { waited <- d.WaitAnswerLeavePending(ctx, challengeAnswer) }()

	challenge, resent := p.challenge(t)
	if ringing {
		p.answer(resent, sip.StatusRinging, "Ringing")
	}
	giveUp(errGaveUp)
	require.ErrorIs(t, <-waited, ErrInvitePending)
	if !ringing {
		require.Same(t, challenge, d.InviteResponse, "the challenge is the last response read")
	}
	return resent
}

// TestDialogClientCancelAfterChallenge has the caller give up while the
// INVITE that was sent again with credentials is pending. The challenge is a
// response to the INVITE before, on a transaction that has ended, so it
// neither answers the pending INVITE nor lets its CANCEL be sent: Cancel
// waits for a provisional to the INVITE sent again (RFC 3261 section 9.1),
// and that wait ends as it does for an INVITE that has had no response.
func TestDialogClientCancelAfterChallenge(t *testing.T) {
	// withdrawn has the peer send a provisional to the INVITE sent again,
	// answer the CANCEL that only then goes out, and end the INVITE with a
	// 487.
	withdrawn := func(t *testing.T, p *scriptedPeer, resent sentRequest) {
		t.Helper()
		p.sentNothing(t)
		p.answer(resent, sip.StatusTrying, "Trying")
		cancelReq := p.next(t, sip.CANCEL)
		wantBranch, _ := resent.req.Via().Params.Get("branch")
		gotBranch, _ := cancelReq.req.Via().Params.Get("branch")
		assert.Equal(t, wantBranch, gotBranch, "the CANCEL is of the pending INVITE's transaction")
		p.answer(cancelReq, sip.StatusOK, "OK")
		p.answer(resent, sip.StatusRequestTerminated, "Request Terminated")
	}

	t.Run("NoCancelBeforeAProvisional", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			resent := leaveChallengedPending(t, p, d, false)

			cancelled := cancelling(t.Context(), d)
			withdrawn(t, p, resent)

			require.NoError(t, <-cancelled)
			assert.Equal(t, sip.StatusRequestTerminated, d.InviteResponse.StatusCode)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("AfterAProvisional", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			resent := leaveChallengedPending(t, p, d, true)

			cancelled := cancelling(t.Context(), d)
			cancelReq := p.next(t, sip.CANCEL)
			p.answer(cancelReq, sip.StatusOK, "OK")
			p.answer(resent, sip.StatusRequestTerminated, "Request Terminated")

			require.NoError(t, <-cancelled)
			assert.Equal(t, sip.StatusRequestTerminated, d.InviteResponse.StatusCode)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("AnsweredBeforeAProvisional", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			resent := leaveChallengedPending(t, p, d, false)

			cancelled := cancelling(t.Context(), d)
			p.sentNothing(t)
			final := p.answer(resent, sip.StatusOK, "OK")

			var answer *ErrAnsweredAfterCancel
			require.ErrorAs(t, <-cancelled, &answer)
			assert.Same(t, final, answer.Res)
			require.NoError(t, answer.Err)
			assert.Equal(t, sip.DialogStateEstablished, d.LoadState())
			assert.Same(t, final, d.InviteResponse)
			assert.False(t, inviteEnded(d), "the INVITE transaction is kept for the ACK")
			p.sentNothing(t)
		})
	})

	t.Run("RefusedBeforeAProvisional", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			resent := leaveChallengedPending(t, p, d, false)

			cancelled := cancelling(t.Context(), d)
			p.sentNothing(t)
			final := p.answer(resent, sip.StatusBusyHere, "Busy Here")

			var refusal *ErrDialogResponse
			require.ErrorAs(t, <-cancelled, &refusal)
			assert.Same(t, final, refusal.Res)
			assert.Same(t, final, d.InviteResponse)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("TheInviteIsNotAnswered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			p, d := newScriptedDialog(t)
			leaveChallengedPending(t, p, d, false)

			err := <-cancelling(t.Context(), d)
			require.ErrorIs(t, err, sip.ErrTransactionTimeout)
			assert.Equal(t, sip.Timer_B, time.Since(start), "the wait ends on Timer B of the INVITE sent again")
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("TheContextEnds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			leaveChallengedPending(t, p, d, false)
			ctx, stop := context.WithCancelCause(t.Context())
			defer stop(nil)

			start := time.Now()
			cancelled := cancelling(ctx, d)
			p.sentNothing(t)
			stop(errOwnerStopped)

			err := <-cancelled
			require.ErrorIs(t, err, errOwnerStopped)
			assert.Zero(t, time.Since(start), "Cancel ends with its context, ahead of Timer B")
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("WaitAnswerWithdraws", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			ctx, giveUp := context.WithCancelCause(t.Context())
			defer giveUp(nil)
			answered := make(chan error, 1)
			go func() { answered <- d.WaitAnswer(ctx, challengeAnswer) }()
			_, resent := p.challenge(t)

			giveUp(errGaveUp)
			withdrawn(t, p, resent)

			require.ErrorIs(t, <-answered, context.Canceled)
			assert.Equal(t, sip.StatusRequestTerminated, d.InviteResponse.StatusCode)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})
}

// TestDialogClientCancelAnsweredAckBye has the 2xx cross the CANCEL of Cancel
// on the wire. The INVITE transaction Cancel keeps has the caller's Ack reach
// the peer and a retransmitted 2xx acknowledged again (RFC 3261 section
// 13.2.2.4), and the caller's Bye ends the call.
func TestDialogClientCancelAnsweredAckBye(t *testing.T) {
	u := newRawUAS(t)
	d := newRawClientDialog(t, u)
	invite := u.expect(t, sip.INVITE)
	u.respond(t, invite, sip.StatusRinging)

	ctx, giveUp := context.WithCancelCause(t.Context())
	defer giveUp(nil)
	err := d.WaitAnswerLeavePending(ctx, AnswerOptions{OnResponse: func(res *sip.Response) error {
		if res.StatusCode == sip.StatusRinging {
			giveUp(errGaveUp)
		}
		return nil
	}})
	require.ErrorIs(t, err, ErrInvitePending)
	require.ErrorIs(t, err, errGaveUp)

	callCtx, callCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer callCancel()
	cancelled := cancelling(callCtx, d)
	cancelReq := u.expect(t, sip.CANCEL)
	final := u.respond(t, invite, sip.StatusOK)
	u.respond(t, cancelReq, sip.StatusCallTransactionDoesNotExists)

	var answer *ErrAnsweredAfterCancel
	require.ErrorAs(t, <-cancelled, &answer)
	assert.Equal(t, sip.StatusOK, answer.Res.StatusCode)
	assert.Equal(t, sip.DialogStateEstablished, d.LoadState())

	require.NoError(t, d.Ack(callCtx))
	u.expect(t, sip.ACK)

	// A retransmission of the 2xx is acknowledged again.
	u.send(t, final, invite.src)
	u.expect(t, sip.ACK)

	bye := make(chan error, 1)
	go func() { bye <- d.Bye(callCtx) }()
	u.respond(t, u.expect(t, sip.BYE), sip.StatusOK)
	require.NoError(t, <-bye)
}

// waitingAnswer starts WaitAnswer on ctx and returns the channel its result
// arrives on.
func waitingAnswer(ctx context.Context, d *DialogClientSession) <-chan error {
	answered := make(chan error, 1)
	go func() { answered <- d.WaitAnswer(ctx, AnswerOptions{}) }()
	return answered
}

// TestDialogClientWaitAnswerContextEnds has the context of WaitAnswer end
// before a final response. WaitAnswer has no context that outlives it, so it
// withdraws the INVITE itself, and that withdrawal ends on the timers of RFC
// 3261 alone: with the CANCEL answered and no final for the INVITE it returns
// the context's error 64*T1 later (section 9.1). A refusal that comes before
// a provisional let a CANCEL be sent is not reported as the context's end,
// and a context ended with WaitAnswerForceCancelErr sends no CANCEL at all.
// The INVITE transaction is terminated each time.
func TestDialogClientWaitAnswerContextEnds(t *testing.T) {
	t.Run("NoFinalWithin64T1", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			ctx, giveUp := context.WithCancelCause(t.Context())
			defer giveUp(nil)
			answered := waitingAnswer(ctx, d)
			p.answer(p.next(t, sip.INVITE), sip.StatusRinging, "Ringing")

			start := time.Now()
			giveUp(errGaveUp)
			p.answer(p.next(t, sip.CANCEL), sip.StatusOK, "OK")

			err := <-answered
			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, 64*sip.T1, time.Since(start))
			var answer *ErrAnsweredAfterCancel
			assert.False(t, errors.As(err, &answer), "no 2xx answered the INVITE")
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("RefusedBeforeProvisional", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			ctx, giveUp := context.WithCancelCause(t.Context())
			defer giveUp(nil)
			answered := waitingAnswer(ctx, d)
			invite := p.next(t, sip.INVITE)

			giveUp(errGaveUp)
			p.sentNothing(t)
			final := p.answer(invite, sip.StatusBusyHere, "Busy Here")

			err := <-answered
			require.Error(t, err)
			assert.NotErrorIs(t, err, context.Canceled, "the refusal is not the context's end")
			var answer *ErrAnsweredAfterCancel
			assert.False(t, errors.As(err, &answer), "no 2xx answered the INVITE")
			assert.Same(t, final, d.InviteResponse)
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})

	t.Run("ForcedCancel", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p, d := newScriptedDialog(t)
			ctx, giveUp := context.WithCancelCause(t.Context())
			defer giveUp(nil)
			answered := waitingAnswer(ctx, d)
			p.answer(p.next(t, sip.INVITE), sip.StatusRinging, "Ringing")

			start := time.Now()
			giveUp(WaitAnswerForceCancelErr)

			err := <-answered
			require.ErrorIs(t, err, context.Canceled)
			assert.Zero(t, time.Since(start), "no CANCEL is waited for")
			assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
			p.sentNothing(t)
		})
	})
}
