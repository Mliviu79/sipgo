//go:build go1.25

package sipgo

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// peerContact is the Contact the scripted peer's responses to an INVITE
// carry: the remote target of the dialog, which a request inside it is sent
// to.
var peerContact = sip.Uri{User: "uas", Host: "127.0.0.1", Port: 5090}

// sendInDialog has the dialog send a request of the given method to the peer
// with Do, has the peer answer it 200, and returns the request the peer read.
func sendInDialog(t *testing.T, p *scriptedPeer, d *DialogClientSession, method sip.RequestMethod) sentRequest {
	t.Helper()
	ctx := t.Context()
	done := make(chan error, 1)
	go func() {
		_, err := d.Do(ctx, sip.NewRequest(method, peerContact))
		done <- err
	}()
	r := p.next(t, method)
	p.answer(r, sip.StatusOK, "OK")
	require.NoError(t, <-done)
	return r
}

// TestDialogClientCancelCSeq has the caller send a request inside the early
// dialog a 180 created, and then withdraw the INVITE. That request takes the
// dialog's next CSeq number (RFC 3261 section 12.2.1.1). The CANCEL is no
// request of that sequence: its CSeq number is that of the INVITE it cancels
// (section 9.1), the one sent again with credentials when the peer challenged
// the first, and it leaves the dialog's sequence where the request before it
// put it.
func TestDialogClientCancelCSeq(t *testing.T) {
	tests := []struct {
		name string
		// challenged has the peer challenge the INVITE first, so the INVITE
		// the CANCEL is of is the one sent again with credentials, which has
		// a CSeq number one higher.
		challenged bool
	}{
		{name: "AfterARequestInTheEarlyDialog"},
		{name: "AfterAChallenge", challenged: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				ctx, giveUp := context.WithCancelCause(t.Context())
				defer giveUp(nil)
				waited := make(chan error, 1)
				go func() { waited <- d.WaitAnswerLeavePending(ctx, challengeAnswer) }()

				invite := p.next(t, sip.INVITE)
				if tc.challenged {
					first := invite.req.CSeq().SeqNo
					p.deliver(invite, p.challengeTo(invite))
					invite = p.next(t, sip.INVITE)
					require.Equal(t, first+1, invite.req.CSeq().SeqNo, "the INVITE sent again takes the next CSeq number")
				}
				want := wireOf(invite.req)
				p.answer(invite, sip.StatusRinging, "Ringing")
				giveUp(errGaveUp)
				require.ErrorIs(t, <-waited, ErrInvitePending)

				update := sendInDialog(t, p, d, sip.UPDATE)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: want.cseq + 1, MethodName: sip.UPDATE}, update.req.CSeq(),
					"the request inside the early dialog takes the next CSeq number")

				cancelled := cancelling(t.Context(), d)
				cancelReq := p.next(t, sip.CANCEL)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: want.cseq, MethodName: sip.CANCEL}, cancelReq.req.CSeq(),
					"the CANCEL has the CSeq number of the INVITE it cancels")
				assert.Equal(t, want, wireOf(cancelReq.req))
				p.answer(cancelReq, sip.StatusOK, "OK")
				p.answer(invite, sip.StatusRequestTerminated, "Request Terminated")

				require.NoError(t, <-cancelled)
				assert.Equal(t, want.cseq+1, d.CSEQ(), "the CANCEL leaves the dialog's sequence where the UPDATE put it")
				assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientAckCSeq has the caller send a request inside the dialog
// before it acknowledges the 2xx of the INVITE: inside the early dialog a 180
// created, as a PRACK or an UPDATE is, or between the 2xx and the ACK. That
// request takes the dialog's next CSeq number (RFC 3261 section 12.2.1.1). The
// ACK is no request of that sequence: its CSeq number is that of the INVITE it
// acknowledges (section 13.2.2.4), the one sent again with credentials when
// the peer challenged the first, and the request after it follows the one
// before it.
func TestDialogClientAckCSeq(t *testing.T) {
	tests := []struct {
		name string
		// challenged has the peer challenge the INVITE first, so the INVITE
		// the 2xx answers is the one sent again with credentials, which has a
		// CSeq number one higher.
		challenged bool
		// early has the caller send an UPDATE when the 180 comes, inside the
		// early dialog.
		early bool
		// answered has the caller send an INFO between the 2xx and the ACK.
		answered bool
	}{
		{name: "WithNoRequestBefore"},
		{name: "AfterARequestInTheEarlyDialog", early: true},
		{name: "AfterARequestSinceTheAnswer", answered: true},
		{name: "AfterBoth", early: true, answered: true},
		{name: "AfterAChallenge", challenged: true, early: true, answered: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				ctx := t.Context()
				opts := challengeAnswer
				// The UPDATE is sent where a caller sends a PRACK: on the
				// provisional, from the goroutine that waits for the answer.
				opts.OnResponse = func(res *sip.Response) error {
					if !tc.early || res.StatusCode != sip.StatusRinging {
						return nil
					}
					_, err := d.Do(ctx, sip.NewRequest(sip.UPDATE, peerContact))
					return err
				}
				waited := make(chan error, 1)
				go func() { waited <- d.WaitAnswer(ctx, opts) }()

				invite := p.next(t, sip.INVITE)
				if tc.challenged {
					p.deliver(invite, p.challengeTo(invite))
					invite = p.next(t, sip.INVITE)
				}
				inviteSeqNo := invite.req.CSeq().SeqNo
				lastSeqNo := inviteSeqNo

				p.answer(invite, sip.StatusRinging, "Ringing")
				if tc.early {
					update := p.next(t, sip.UPDATE)
					lastSeqNo++
					assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo, MethodName: sip.UPDATE}, update.req.CSeq())
					p.answer(update, sip.StatusOK, "OK")
				}
				p.answer(invite, sip.StatusOK, "OK")
				require.NoError(t, <-waited)
				if tc.answered {
					info := sendInDialog(t, p, d, sip.INFO)
					lastSeqNo++
					assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo, MethodName: sip.INFO}, info.req.CSeq())
				}

				require.NoError(t, d.Ack(ctx))
				ack := p.next(t, sip.ACK)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: inviteSeqNo, MethodName: sip.ACK}, ack.req.CSeq(),
					"the ACK has the CSeq number of the INVITE it acknowledges")
				assert.Equal(t, lastSeqNo, d.CSEQ(), "the ACK leaves the dialog's sequence where the request before it put it")

				info := sendInDialog(t, p, d, sip.INFO)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo + 1, MethodName: sip.INFO}, info.req.CSeq(),
					"the request after the ACK takes the next CSeq number")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogClientReInviteAckCSeq has the caller acknowledge the 2xx of a
// re-INVITE, which it sent with Do and acknowledges with WriteAck. The ACK's
// CSeq number is that of the re-INVITE (RFC 3261 section 13.2.2.4): of the one
// sent again with the dialog's credentials when the peer challenged the
// first, and not of a request the caller sent between the 2xx and the ACK,
// which takes the dialog's next number.
func TestDialogClientReInviteAckCSeq(t *testing.T) {
	tests := []struct {
		name string
		// challenged has the peer challenge the re-INVITE, so the one the 2xx
		// answers is sent again with credentials and a CSeq number one higher.
		challenged bool
		// answered has the caller send an INFO between the 2xx and the ACK.
		answered bool
	}{
		{name: "WithNoRequestBefore"},
		{name: "AfterARequestSinceTheAnswer", answered: true},
		{name: "AfterAChallenge", challenged: true},
		{name: "AfterAChallengeAndARequest", challenged: true, answered: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t)
				d.SetCredentials(testAuth)
				ctx := t.Context()
				invite := p.next(t, sip.INVITE)
				p.answer(invite, sip.StatusOK, "OK")
				require.NoError(t, d.WaitAnswer(ctx, AnswerOptions{}))
				require.NoError(t, d.Ack(ctx))
				p.next(t, sip.ACK)
				lastSeqNo := invite.req.CSeq().SeqNo

				reInvited := make(chan error, 1)
				go func() {
					_, err := d.Do(ctx, sip.NewRequest(sip.INVITE, peerContact))
					reInvited <- err
				}()
				reInvite := p.next(t, sip.INVITE)
				lastSeqNo++
				if tc.challenged {
					p.deliver(reInvite, p.challengeTo(reInvite))
					reInvite = p.next(t, sip.INVITE)
					lastSeqNo++
				}
				reInviteSeqNo := lastSeqNo
				require.Equal(t, &sip.CSeqHeader{SeqNo: reInviteSeqNo, MethodName: sip.INVITE}, reInvite.req.CSeq())
				p.answer(reInvite, sip.StatusOK, "OK")
				require.NoError(t, <-reInvited)
				if tc.answered {
					info := sendInDialog(t, p, d, sip.INFO)
					lastSeqNo++
					assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo, MethodName: sip.INFO}, info.req.CSeq())
				}

				require.NoError(t, d.WriteAck(ctx, sip.NewRequest(sip.ACK, peerContact)))
				ack := p.next(t, sip.ACK)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: reInviteSeqNo, MethodName: sip.ACK}, ack.req.CSeq(),
					"the ACK has the CSeq number of the re-INVITE it acknowledges")
				assert.Equal(t, lastSeqNo, d.CSEQ(), "the ACK leaves the dialog's sequence where the request before it put it")
				p.sentNothing(t)
			})
		})
	}
}
