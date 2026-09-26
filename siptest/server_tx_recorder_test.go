package siptest

import (
	"testing"
	"time"

	"github.com/emiago/sipgo/sip"
	"github.com/stretchr/testify/require"
)

// TestServerTxRecorderResultWhileResponding reads the recorded responses while
// a response is written from another goroutine, as a test does that checks
// Result while a dialog answers on a goroutine of its own.
func TestServerTxRecorderResultWhileResponding(t *testing.T) {
	invite := sip.NewRequest(sip.INVITE, sip.Uri{User: "bob", Host: "127.0.0.1", Port: 5060})
	via := &sip.ViaHeader{
		ProtocolName:    "SIP",
		ProtocolVersion: "2.0",
		Transport:       "UDP",
		Host:            "127.0.0.2",
		Port:            5060,
		Params:          sip.NewParams(),
	}
	via.Params.Add("branch", sip.GenerateBranch())
	invite.AppendHeader(via)
	invite.AppendHeader(&sip.FromHeader{Address: sip.Uri{User: "alice", Host: "127.0.0.2"}, Params: sip.NewParams()})
	invite.AppendHeader(&sip.ToHeader{Address: sip.Uri{User: "bob", Host: "127.0.0.1"}, Params: sip.NewParams()})
	callID := sip.CallIDHeader("recorder-test")
	invite.AppendHeader(&callID)
	invite.AppendHeader(&sip.CSeqHeader{SeqNo: 1, MethodName: sip.INVITE})

	rec := NewServerTxRecorder(invite)
	defer rec.Terminate()

	responded := make(chan error, 1)
	go func() {
		responded <- rec.Respond(sip.NewResponseFromRequest(invite, sip.StatusRinging, "Ringing", nil))
	}()
	// Read while the response may still be written.
	_ = rec.Result()

	select {
	case err := <-responded:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the response was not written")
	}
	res := rec.Result()
	require.Len(t, res, 1)
	require.Equal(t, sip.StatusRinging, res[0].StatusCode)
}
