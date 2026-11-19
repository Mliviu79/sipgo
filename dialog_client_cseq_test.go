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
