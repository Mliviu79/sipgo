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

const (
	// challengeSource is the address the peer's challenge comes from, which
	// is not the one the INVITE's Request-URI resolves to.
	challengeSource = "192.0.2.10:5062"
	// ringingSource is the address the peer's 180 comes from, which is
	// neither challengeSource nor the one the Request-URI resolves to.
	ringingSource = "192.0.2.20:5064"
)

var (
	// looseRouters are the Record-Route values of two proxies that route
	// loosely, as a response carries them: the proxy nearest the peer first.
	looseRouters = []string{"<sip:p2.example.com;lr>", "<sip:p1.example.com;lr>"}
	// strictRouter is the Record-Route value of a proxy that routes strictly:
	// a route set that starts with it puts its URI in the Request-URI (RFC
	// 3261 section 12.2.1.1).
	strictRouter = []string{"<sip:p1.example.com>"}
	// outboundProxy is a pre-existing route set of one URI, as RFC 3261
	// section 8.1.1.1 has a UA with an outbound proxy configured.
	outboundProxy = []string{"<sip:outbound.example.com;lr>"}
)

// wire holds what tells which request a request is and where it is sent: the
// parts a CANCEL takes unchanged from the request it cancels (RFC 3261
// section 9.1), and all but the CSeq number and the Via that an INVITE sent
// again with credentials keeps of the INVITE before (sections 8.1.3.5 and
// 22.2).
type wire struct {
	requestURI  string
	routes      []string
	destination string
	transport   string
	callID      string
	from        string
	to          string
	cseq        uint32
	vias        []string
}

// wireOf returns the wire of req as it is now. The dialog sends its INVITE
// again as the same request, so a test takes the wire of an INVITE before the
// peer answers it.
func wireOf(req *sip.Request) wire {
	return wire{
		requestURI:  req.Recipient.String(),
		routes:      headerValues(req, "Route"),
		destination: req.Destination(),
		transport:   req.Transport(),
		callID:      req.CallID().Value(),
		from:        req.From().Value(),
		to:          req.To().Value(),
		cseq:        req.CSeq().SeqNo,
		vias:        headerValues(req, "Via"),
	}
}

// headerValues returns the values of the header fields of req with the given
// name, in the order req carries them.
func headerValues(req *sip.Request, name string) []string {
	var values []string
	for _, h := range req.GetHeaders(name) {
		values = append(values, h.Value())
	}
	return values
}

// headersOf returns a header field of the given name for each value.
func headersOf(name string, values []string) []sip.Header {
	var headers []sip.Header
	for _, v := range values {
		headers = append(headers, sip.NewHeader(name, v))
	}
	return headers
}

// TestDialogClientChallengeRecordRoute has the peer's digest challenge carry
// Record-Route. A 401 establishes no dialog (RFC 3261 section 12.1), so it
// gives no route set: the INVITE sent again with credentials is the request
// before with a CSeq one higher, on a new transaction (sections 8.1.3.5 and
// 22.2), and keeps its Request-URI and the route it had, the pre-existing
// route set its caller gave it or none (section 8.1.1.1). Where it is sent
// stays the UA's policy (section 8.1.2): with RewriteContact that is the
// source of the last response, the challenge.
func TestDialogClientChallengeRecordRoute(t *testing.T) {
	tests := []struct {
		name string
		// route holds the Route values the caller gives the INVITE.
		route []string
		// recordRoutes holds the Record-Route values of the challenge.
		recordRoutes []string
		// rewriteContact is the UA's RewriteContact.
		rewriteContact bool
	}{
		{name: "LooseRouters", recordRoutes: looseRouters},
		{name: "AStrictRouter", recordRoutes: strictRouter},
		{name: "OverAPreExistingRoute", route: outboundProxy, recordRoutes: looseRouters},
		{name: "RewriteContact", recordRoutes: looseRouters, rewriteContact: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t, headersOf("Route", tc.route)...)
				d.UA.RewriteContact = tc.rewriteContact
				ctx, giveUp := context.WithCancelCause(t.Context())
				defer giveUp(nil)
				waited := make(chan error, 1)
				go func() { waited <- d.WaitAnswerLeavePending(ctx, challengeAnswer) }()

				first := p.next(t, sip.INVITE)
				want := wireOf(first.req)
				require.Equal(t, tc.route, want.routes, "the INVITE carries the route its caller gave it")
				challenge := p.challengeTo(first)
				for _, h := range headersOf("Record-Route", tc.recordRoutes) {
					challenge.AppendHeader(h)
				}
				challenge.SetSource(challengeSource)
				p.deliver(first, challenge)

				resent := p.next(t, sip.INVITE)
				require.NotNil(t, resent.req.GetHeader("Authorization"), "the INVITE is sent again with credentials")
				got := wireOf(resent.req)
				assert.NotEqual(t, want.vias, got.vias, "the INVITE sent again is a new transaction")
				want.vias = got.vias
				want.cseq++
				if tc.rewriteContact {
					want.destination = challengeSource
				}
				assert.Equal(t, want, got)

				giveUp(errGaveUp)
				require.ErrorIs(t, <-waited, ErrInvitePending)
			})
		})
	}
}

// TestDialogClientCancelRecordRoute has the caller withdraw an INVITE whose
// provisional response carries Record-Route, or comes from another address
// than the INVITE was sent to. The CANCEL is matched with the INVITE and
// routed as it was, hop by hop, so it is the INVITE's Request-URI, Call-ID,
// To, From, CSeq number, single Via and Route values, sent where the INVITE
// was sent (RFC 3261 section 9.1): the route set of the early dialog the
// provisional creates is for requests inside that dialog, which a CANCEL is
// not, and RewriteContact does not move it to the provisional's source.
func TestDialogClientCancelRecordRoute(t *testing.T) {
	tests := []struct {
		name string
		// route holds the Route values the caller gives the INVITE.
		route []string
		// challenged has the peer challenge the INVITE first, from
		// challengeSource, so the INVITE the CANCEL is of is the one sent
		// again with credentials.
		challenged bool
		// recordRoutes holds the Record-Route values of the 180.
		recordRoutes []string
		// rewriteContact is the UA's RewriteContact.
		rewriteContact bool
	}{
		{name: "LooseRouters", recordRoutes: looseRouters},
		{name: "AStrictRouter", recordRoutes: strictRouter},
		{name: "OverAPreExistingRoute", route: outboundProxy, recordRoutes: looseRouters},
		{name: "RewriteContact", rewriteContact: true},
		{name: "RewriteContactOverAPreExistingRoute", route: outboundProxy, rewriteContact: true},
		{name: "AfterAChallenge", challenged: true, recordRoutes: looseRouters},
		{name: "AfterAChallengeUnderRewriteContact", challenged: true, rewriteContact: true},
		{name: "AfterAChallengeUnderRewriteContactWithRecordRoute", challenged: true, recordRoutes: strictRouter, rewriteContact: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				p, d := newScriptedDialog(t, headersOf("Route", tc.route)...)
				d.UA.RewriteContact = tc.rewriteContact
				ctx, giveUp := context.WithCancelCause(t.Context())
				defer giveUp(nil)
				waited := make(chan error, 1)
				go func() { waited <- d.WaitAnswerLeavePending(ctx, challengeAnswer) }()

				invite := p.next(t, sip.INVITE)
				if tc.challenged {
					challenge := p.challengeTo(invite)
					challenge.SetSource(challengeSource)
					p.deliver(invite, challenge)
					invite = p.next(t, sip.INVITE)
				}
				want := wireOf(invite.req)
				ringing := p.response(invite, sip.StatusRinging, "Ringing")
				for _, h := range headersOf("Record-Route", tc.recordRoutes) {
					ringing.AppendHeader(h)
				}
				ringing.SetSource(ringingSource)
				p.deliver(invite, ringing)
				giveUp(errGaveUp)
				require.ErrorIs(t, <-waited, ErrInvitePending)

				cancelled := cancelling(t.Context(), d)
				cancelReq := p.next(t, sip.CANCEL)
				assert.Equal(t, want, wireOf(cancelReq.req))
				p.answer(cancelReq, sip.StatusOK, "OK")
				p.answer(invite, sip.StatusRequestTerminated, "Request Terminated")

				require.NoError(t, <-cancelled)
				assert.True(t, inviteEnded(d), "the INVITE transaction is terminated")
				p.sentNothing(t)
			})
		})
	}
}
