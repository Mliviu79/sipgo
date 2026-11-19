//go:build go1.25

package sipgo

import (
	"testing"
	"testing/synctest"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newScriptedServerDialog returns a confirmed dialog that we answered, and the
// scriptedPeer that holds each request the dialog sends inside it.
func newScriptedServerDialog(t *testing.T) (*scriptedPeer, *DialogServerSession) {
	t.Helper()
	p, client := newScriptedPeer(t)
	return p, newConfirmedServerDialog(t, client, peerContact)
}

// TestDialogServerReInviteAckCSeq has the side that answered the dialog
// acknowledge the 2xx of a re-INVITE of its own, which it sent with Do and
// acknowledges with WriteRequest. The ACK's CSeq number is that of the
// re-INVITE (RFC 3261 section 13.2.2.4): of the one sent again with the
// dialog's credentials when the peer challenged the first, and not of a
// request sent between the 2xx and the ACK, which takes the dialog's next
// number (section 12.2.1.1). The ACK is no request of that sequence: the
// request after it follows the one before it.
func TestDialogServerReInviteAckCSeq(t *testing.T) {
	tests := []struct {
		name string
		// challenged has the peer challenge the re-INVITE, so the one the 2xx
		// answers is sent again with credentials and a CSeq number one higher.
		challenged bool
		// answered has an INFO sent between the 2xx and the ACK.
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
				p, d := newScriptedServerDialog(t)
				d.SetCredentials(testAuth)
				ctx := t.Context()
				lastSeqNo := d.CSEQ()

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

				require.NoError(t, d.WriteRequest(sip.NewRequest(sip.ACK, peerContact)))
				ack := p.next(t, sip.ACK)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: reInviteSeqNo, MethodName: sip.ACK}, ack.req.CSeq(),
					"the ACK has the CSeq number of the re-INVITE it acknowledges")
				assert.Equal(t, lastSeqNo, d.CSEQ(), "the ACK leaves the dialog's sequence where the request before it put it")

				info := sendInDialog(t, p, d, sip.INFO)
				assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo + 1, MethodName: sip.INFO}, info.req.CSeq(),
					"the request after the ACK takes the next CSeq number")
				p.sentNothing(t)
			})
		})
	}
}

// TestDialogServerAckWithNoInviteSent has the side that answered the dialog
// build an ACK when it has sent no INVITE inside it, so there is none the ACK
// acknowledges. The ACK is numbered with the dialog's last number, never
// zero, and leaves that number as it is.
func TestDialogServerAckWithNoInviteSent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, d := newScriptedServerDialog(t)
		lastSeqNo := d.CSEQ()
		require.NotZero(t, lastSeqNo)

		require.NoError(t, d.WriteRequest(sip.NewRequest(sip.ACK, peerContact)))
		ack := p.next(t, sip.ACK)
		assert.Equal(t, &sip.CSeqHeader{SeqNo: lastSeqNo, MethodName: sip.ACK}, ack.req.CSeq())
		assert.Equal(t, lastSeqNo, d.CSEQ())
		p.sentNothing(t)
	})
}
