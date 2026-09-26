package sipgo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/icholy/digest"
)

type DialogServerSession struct {
	Dialog
	inviteTx sip.ServerTransaction
	// s        *DialogServer
	ua *DialogUA

	onClose func()
}

// ReadAck changes dialog state to confiremed
func (s *DialogServerSession) ReadAck(req *sip.Request, tx sip.ServerTransaction) error {
	// cseq must match to our last dialog cseq
	if req.CSeq().SeqNo != s.remoteCSeqNo.Load() {
		return ErrDialogInvalidCseq
	}
	s.setState(sip.DialogStateConfirmed)
	return nil
}

// ReadBye answers the peer's BYE and ends the dialog. A BYE below the remote
// sequence number, which a request of the peer read before it, such as a
// re-INVITE, set, is out of order: it is answered 500, ErrDialogInvalidCseq is
// returned and the dialog goes on (RFC 3261 section 12.2.2). The remote
// sequence number is left as it is, so the ACK to our 2xx, read after the BYE
// when it is overtaken, still matches it.
func (s *DialogServerSession) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	if err := s.refuseOutOfOrder(req, tx); err != nil {
		return err
	}

	defer s.Close()
	defer s.inviteTx.Terminate() // Terminat`es Invite transaction

	res := sip.NewResponseFromRequest(req, 200, "OK", nil)
	err := tx.Respond(res)
	// The dialog ends whether or not the 200 is sent: it is closed and its
	// INVITE transaction terminated either way.
	s.setState(sip.DialogStateEnded)
	return err
}

// Do does request response pattern. For more control over transaction use TransactionRequest
func (s *DialogServerSession) Do(ctx context.Context, req *sip.Request) (*sip.Response, error) {
	tx, err := s.TransactionRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer tx.Terminate()
	for {
		select {
		case res := <-tx.Responses():
			if res.IsProvisional() {
				continue
			}
			return res, nil

		case <-tx.Done():
			return nil, tx.Err()

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// TransactionRequest is doing client DIALOG request based on RFC
// https://www.rfc-editor.org/rfc/rfc3261#section-12.2.1
// This ensures that you have proper request done within dialog
func (s *DialogServerSession) TransactionRequest(ctx context.Context, req *sip.Request) (sip.ClientTransaction, error) {
	s.buildReq(req)
	// Passing option to avoid CSEQ apply
	return s.ua.Client.TransactionRequest(ctx, req, func(c *Client, req *sip.Request) error {
		if req.Via() == nil {
			ClientRequestAddVia(c, req)
		}
		// Makes sure Content-Length is present
		if req.Body() == nil {
			req.SetBody(nil)
		}
		return nil
	})
}

func (s *DialogServerSession) WriteRequest(req *sip.Request) error {
	s.buildReq(req)
	return s.ua.Client.WriteRequest(req)
}

func (s *DialogServerSession) buildReq(req *sip.Request) {
	// Keep any request inside dialog
	mustHaveHeaders := make([]sip.Header, 0, 5)
	if h, invH := req.From(), s.InviteResponse; h == nil && invH != nil {
		hh := invH.To().AsFrom()
		mustHaveHeaders = append(mustHaveHeaders, &hh)
	}

	if h, invH := req.To(), s.InviteRequest.From(); h == nil {
		hh := invH.AsTo()
		mustHaveHeaders = append(mustHaveHeaders, &hh)
	}

	if h, invH := req.CallID(), s.InviteRequest.CallID(); h == nil {
		mustHaveHeaders = append(mustHaveHeaders, sip.HeaderClone(invH))
	}

	if h := req.MaxForwards(); h == nil {
		maxFwd := sip.MaxForwardsHeader(70)
		mustHaveHeaders = append(mustHaveHeaders, &maxFwd)
	}

	cseq := req.CSeq()
	if cseq == nil {
		cseq = &sip.CSeqHeader{
			SeqNo:      s.InviteRequest.CSeq().SeqNo,
			MethodName: req.Method,
		}
		mustHaveHeaders = append(mustHaveHeaders, cseq)
	}
	if len(mustHaveHeaders) > 0 {
		req.PrependHeader(mustHaveHeaders...)
	}

	// For safety make sure we are starting with our last dialog cseq num
	cseq.SeqNo = s.lastCSeqNo.Load()

	if !req.IsAck() && !req.IsCancel() {
		// Do cseq increment within dialog
		cseq.SeqNo++
	}

	// https://datatracker.ietf.org/doc/html/rfc3261#section-16.12.1.2
	rrs := s.InviteRequest.GetHeaders("Record-Route")
	for i := range rrs {
		recordRoute := rrs[i]
		req.AppendHeader(sip.NewHeader("Route", recordRoute.Value()))
	}

	// Check Route Header
	// Should be handled by transport layer but here we are making this explicit
	if rr := req.Route(); rr != nil {
		req.SetDestination(rr.Address.HostPort())
	}
	// TODO check correct behavior strict routing vs loose routing
	// recordRoute := req.RecordRoute()
	// if recordRoute != nil {
	// 	if recordRoute.Address.UriParams.Has("lr") {
	// 		bye.AppendHeader(&sip.RouteHeader{Address: recordRoute.Address})
	// 	} else {
	// 		/* TODO
	// 		   If the route set is not empty, and its first URI does not contain the
	// 		   lr parameter, the UAC MUST place the first URI from the route set
	// 		   into the Request-URI, stripping any parameters that are not allowed
	// 		   in a Request-URI.  The UAC MUST add a Route header field containing
	// 		   the remainder of the route set values in order, including all
	// 		   parameters.  The UAC MUST then place the remote target URI into the
	// 		   Route header field as the last value.
	// 		*/
	// 	}
	// }

	s.lastCSeqNo.Store(cseq.SeqNo)

	if h := req.Contact(); h == nil {
		req.AppendHeader(sip.HeaderClone(&s.ua.ContactHDR))
	}

	if s.ua.RewriteContact && len(rrs) == 0 {
		req.SetDestination(s.InviteRequest.Source())
	}

	// TODO check is contact header routable
	// If not then we should force destination as source address
	req.SetTransport(s.InviteRequest.Transport())
}

// Close is always good to call for cleanup or terminating dialog state
func (s *DialogServerSession) Close() error {
	if s.onClose != nil {
		s.onClose()
	}
	return nil
}

// Respond should be called for Invite request, you may want to call this multiple times like
// 100 Progress or 180 Ringing
// 2xx for creating dialog or other code in case failure
//
// In case Cancel request received: ErrDialogCanceled is responded
func (s *DialogServerSession) Respond(statusCode int, reason string, body []byte, headers ...sip.Header) error {
	// Must copy Record-Route headers. Done by this command
	res := sip.NewResponseFromRequest(s.InviteRequest, statusCode, reason, body)

	for _, h := range headers {
		res.AppendHeader(h)
	}

	return s.WriteResponse(res)
}

// RespondSDP is just wrapper to call 200 with SDP.
// It is better to use this when answering as it provide correct headers
func (s *DialogServerSession) RespondSDP(sdp []byte) error {
	if sdp == nil {
		return fmt.Errorf("sdp not provided")
	}
	res := sip.NewSDPResponseFromRequest(s.InviteRequest, sdp)
	return s.WriteResponse(res)
}

var errDialogUnauthorized = errors.New("unathorized")

func (s *DialogServerSession) authDigest(chal *digest.Challenge, opts digest.Options) error {
	authorized := func() bool {
		authorizationHDR := s.InviteRequest.GetHeader("Authorization")
		if authorizationHDR == nil {
			return false
		}

		hdrVal := authorizationHDR.Value()
		creds, err := digest.ParseCredentials(hdrVal)
		if err != nil {
			return false
		}

		digCred, err := digest.Digest(chal, opts)
		if err != nil {
			return false
		}

		return creds.Response == digCred.Response
	}()

	if authorized {
		return nil
	}

	hdrVal := chal.String()
	hdr := sip.NewHeader("WWW-Authenticate", hdrVal)

	res := sip.NewResponseFromRequest(s.InviteRequest, sip.StatusUnauthorized, "Unauthorized", nil)
	res.AppendHeader(hdr)
	if err := s.WriteResponse(res); err != nil {
		return err
	}

	return errDialogUnauthorized
}

// WriteResponse allows passing you custom response
// NOTE: Make sure you have built response based on dialog.InviteRequest which makes sure
// that dialog ID do match
//
// A 2xx is retransmitted until its ACK is read, which WriteResponse waits for.
// If no ACK arrives within 64*T1, the dialog is confirmed and ErrDialogAckTimeout
// is returned: end the session with Bye (RFC 3261 section 13.3.1.4). If the
// dialog ends first, ErrDialogEndedBeforeAck is returned. If the transaction
// takes no 2xx, as after a CANCEL, its error is returned and the dialog the 2xx
// established ends.
func (s *DialogServerSession) WriteResponse(res *sip.Response) error {
	tx := s.inviteTx

	if res.Contact() == nil {
		// Add our default contact header
		res.AppendHeader(&s.ua.ContactHDR)
	}

	s.Dialog.InviteResponse = res

	// Do we have cancel in meantime
	select {
	case <-tx.Done():
		// There must be some error
		return tx.Err()
	default:
	}

	if !res.IsSuccess() {
		if res.IsProvisional() {
			// This will not create dialog so we will just respond
			return tx.Respond(res)
		}

		// For final response we want to set dialog ended state
		if err := tx.Respond(res); err != nil {
			return err
		}

		// We should wait ACK for cleaner exit
		select {
		case <-tx.Acks():
		case <-tx.Done():
			// This means tx moved to terminated state and no more invite retransmissions is accepted
		}
		s.setState(sip.DialogStateEnded)
		return nil
	}

	id, err := sip.DialogIDFromResponse(res)
	if err != nil {
		return err
	}

	if id != s.Dialog.ID {
		return fmt.Errorf("ID do not match. Invite request has changed headers?")
	}

	established := s.setState(sip.DialogStateEstablished)

	// Register dialog state read channel before transmitting 200 OK. This prevents a race
	// condition where the ACK is received before we start waiting for it.
	readStateCh := s.StateRead()
	// The state is loaded after the read is registered, so a change made in
	// between, such as the dialog ending, is not missed. An ended dialog gets
	// no 2xx. When a CANCEL or the end of the transaction ended it, the
	// transaction's error says so, as in the check above. A dialog already
	// confirmed, answered again, waits for no ACK.
	state := s.LoadState()
	if state == sip.DialogStateEnded {
		if err := tx.Err(); err != nil {
			return err
		}
		return ErrDialogEndedBeforeAck
	}

	// Wait now for ACK for our 2xx
	// https://datatracker.ietf.org/doc/html/rfc3261#section-13.3.1.4
	//
	// If the server retransmits the 2xx response for 64*T1 seconds without
	// receiving an ACK, the dialog is confirmed, but the session SHOULD be
	// terminated.  This is accomplished with a BYE, as described in Section
	// 15.
	//
	// The deadline is armed once, before the first transmission, so it expires
	// before the transaction's Timer L, which starts with that transmission.
	ackTimeout := time.NewTimer(64 * sip.T1)
	defer ackTimeout.Stop()

	if err := tx.Respond(res); err != nil {
		// The transaction took no 2xx, for example after a CANCEL read once
		// the dialog was established, which it answered 487. A dialog this
		// call established is then never answered, and ends with the cause.
		if established {
			s.endWithCause(err)
		}
		return err
	}

	// We are following RFC 6026, which states that this is TU thing and not Transaction layer.
	interval := sip.T1
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for state == sip.DialogStateEstablished {
		select {
		case <-timer.C:
			if err := tx.Respond(res); err != nil {
				// Timer L ends the transaction 64*T1 after the first
				// transmission, just after the deadline. A retransmission
				// that finds the transaction ended by it is past the
				// deadline too, and reports the same timeout.
				select {
				case <-ackTimeout.C:
					return s.ackTimedOut()
				default:
				}
				return err
			}
			// 2xx response is passed to the transport with an
			//    interval that starts at T1 seconds and doubles for each
			//    retransmission until it reaches T2 seconds (T1 and T2 are defined in
			//    Section 17).
			interval = min(2*interval, sip.T2)
			timer.Reset(interval)

		case <-ackTimeout.C:
			return s.ackTimedOut()
		case state = <-readStateCh:
		}
	}
	if state != sip.DialogStateConfirmed {
		return ErrDialogEndedBeforeAck
	}
	return nil
}

// ackTimedOut confirms a dialog whose 2xx got no ACK within 64*T1 and reports
// it with ErrDialogAckTimeout, so the caller ends the session with a BYE.
func (s *DialogServerSession) ackTimedOut() error {
	s.setState(sip.DialogStateConfirmed)
	return ErrDialogAckTimeout
}

func (s *DialogServerSession) Bye(ctx context.Context) error {
	req := s.Dialog.InviteRequest
	cont := s.Dialog.InviteRequest.Contact()
	bye := sip.NewRequest(sip.BYE, cont.Address)
	bye.SetTransport(req.Transport())

	return s.WriteBye(ctx, bye)
}

// WriteBye sends bye, once the ACK to our 2xx is read or the INVITE
// transaction has timed out, and waits within ctx for its answer. The dialog
// ends as soon as bye is passed to its client transaction (RFC 3261 section
// 15.1.1), whatever the answer.
func (s *DialogServerSession) WriteBye(ctx context.Context, bye *sip.Request) error {
	state := s.state.Load()
	// In case dialog terminated
	if sip.DialogState(state) == sip.DialogStateEnded {
		return nil
	}

	// https://datatracker.ietf.org/doc/html/rfc3261#section-15
	// However, the callee's UA MUST NOT send a BYE on a confirmed dialog
	// until it has received an ACK for its 2xx response or until the server
	// transaction times out.
	// if sip.DialogState(state) != sip.DialogStateConfirmed {
	// 	return nil
	// }

	res := s.Dialog.InviteResponse

	if !res.IsSuccess() {
		return fmt.Errorf("can not send bye on NON success response")
	}

	// This is tricky
	defer s.inviteTx.Terminate() // Terminates INVITE in all cases
	if sip.DialogState(state) < sip.DialogStateConfirmed {
		// Wait for the ACK, which confirms the dialog, or for the INVITE
		// transaction to time out. The state is loaded again after the read
		// is registered, so a change made in between is not missed.
		states := s.StateRead()
	wait:
		for state := s.LoadState(); state < sip.DialogStateConfirmed; {
			select {
			case state = <-states:
			case <-s.inviteTx.Done():
				break wait
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// A dialog that ended meanwhile, as on the peer's BYE, needs none.
		if s.LoadState() == sip.DialogStateEnded {
			return nil
		}
	}

	tx, err := s.TransactionRequest(ctx, bye)
	if err != nil {
		return err
	}
	defer tx.Terminate() // Terminates current transaction
	ctx, cancel := s.endOnBye(ctx)
	defer cancel()

	// Wait 200
	select {
	case res := <-tx.Responses():
		if res.StatusCode != 200 {
			return ErrDialogResponse{res}
		}
		return nil
	case <-tx.Done():
		return tx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DialogServerCache serves as quick way to start building dialog server
// It is not optimized version and it is recomended that you build own dialog caching
type DialogServerCache struct {
	dialogs sync.Map
	ua      DialogUA
}

func (s *DialogServerCache) loadDialog(id string) *DialogServerSession {
	val, ok := s.dialogs.Load(id)
	if !ok || val == nil {
		return nil
	}

	t := val.(*DialogServerSession)
	return t
}

func (s *DialogServerCache) MatchDialogRequest(req *sip.Request) (*DialogServerSession, error) {
	id, err := sip.DialogIDFromRequestUAS(req)
	if err != nil {
		return nil, errors.Join(ErrDialogOutsideDialog, err)
	}

	dt := s.loadDialog(id)
	if dt == nil {
		return nil, ErrDialogDoesNotExists
	}
	return dt, nil
}

// NewDialogServerCache provides simple cache layer for managing UAS dialog
// Contact hdr is default that is provided for responses.
// Client is needed for termination dialog session
// In case handling different transports you should have multiple instances per transport
//
// Using DialogUA is now better way for genereting dialogs without caching and giving you as caller whole control of dialog
func NewDialogServerCache(client *Client, contactHDR sip.ContactHeader) *DialogServerCache {
	s := &DialogServerCache{
		dialogs: sync.Map{},
		ua: DialogUA{
			Client:     client,
			ContactHDR: contactHDR,
		},
	}
	return s
}

// ReadInvite should read from your OnInvite handler for which it creates dialog context
// You need to use DialogServerSession for all further responses
// Do not forget to add ReadAck and ReadBye for confirming dialog and terminating
func (s *DialogServerCache) ReadInvite(req *sip.Request, tx sip.ServerTransaction) (*DialogServerSession, error) {
	dtx, err := s.ua.ReadInvite(req, tx)
	if err != nil {
		return nil, err
	}

	id := dtx.ID
	dtx.onClose = func() {
		s.dialogs.Delete(id)
	}
	s.dialogs.Store(id, dtx)
	return dtx, nil
}

// ReadAck should read from your OnAck handler
func (s *DialogServerCache) ReadAck(req *sip.Request, tx sip.ServerTransaction) error {
	dt, err := s.MatchDialogRequest(req)
	if err != nil {
		return err
	}
	return dt.ReadAck(req, tx)
}

// ReadBye should read from your OnBye handler. Returns error if it fails
func (s *DialogServerCache) ReadBye(req *sip.Request, tx sip.ServerTransaction) error {
	dt, err := s.MatchDialogRequest(req)
	if err != nil {
		return err
	}
	return dt.ReadBye(req, tx)
}
