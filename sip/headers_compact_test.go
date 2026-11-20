package sip

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compactHeaderForms lists every compact header name with the full name it
// stands for, in the spelling the full form is registered with.
var compactHeaderForms = []struct {
	compact string
	full    string
}{
	{compact: "a", full: "Accept-Contact"},
	{compact: "b", full: "Referred-By"},
	{compact: "c", full: "Content-Type"},
	{compact: "e", full: "Content-Encoding"},
	{compact: "f", full: "From"},
	{compact: "i", full: "Call-ID"},
	{compact: "k", full: "Supported"},
	{compact: "l", full: "Content-Length"},
	{compact: "m", full: "Contact"},
	{compact: "o", full: "Event"},
	{compact: "r", full: "Refer-To"},
	{compact: "s", full: "Subject"},
	{compact: "t", full: "To"},
	{compact: "u", full: "Allow-Events"},
	{compact: "v", full: "Via"},
	{compact: "x", full: "Session-Expires"},
}

func TestCompactHeaderNameRoundTrip(t *testing.T) {
	for _, form := range compactHeaderForms {
		t.Run(form.full, func(t *testing.T) {
			assert.Equal(t, form.compact, compactHeaderName(form.full))
			assert.Equal(t, HeaderToLower(form.full), headerNameKey(form.compact))
			assert.Equal(t, HeaderToLower(form.full), headerNameKey(strings.ToUpper(form.compact)))
			assert.Equal(t, HeaderToLower(form.full), headerNameKey(form.full))
		})
	}
}

const compactReferRequest = "REFER sip:bob@example.com SIP/2.0\r\n" +
	"v: SIP/2.0/UDP 10.0.0.1:5060;branch=z9hG4bKcompact\r\n" +
	"f: <sip:alice@example.com>;tag=a1\r\n" +
	"t: <sip:bob@example.com>;tag=b1\r\n" +
	"i: compact-call-id\r\n" +
	"CSeq: 2 REFER\r\n" +
	"m: <sip:alice@10.0.0.1:5060>\r\n" +
	"a: *;audio\r\n" +
	"r: <sip:carol@example.com>\r\n" +
	"b: <sip:alice@example.com>\r\n" +
	"o: refer\r\n" +
	"k: replaces\r\n" +
	"s: transfer\r\n" +
	"e: identity\r\n" +
	"u: refer\r\n" +
	"x: 1800;refresher=uas\r\n" +
	"c: text/plain\r\n" +
	"l: 0\r\n" +
	"\r\n"

func TestParseCompactHeaderLookups(t *testing.T) {
	msg, err := ParseMessage([]byte(compactReferRequest))
	require.NoError(t, err)
	req, ok := msg.(*Request)
	require.True(t, ok)

	t.Run("TypedAccessors", func(t *testing.T) {
		require.NotNil(t, req.Via())
		assert.Equal(t, "z9hG4bKcompact", req.Via().Params.GetOr("branch", ""))
		require.NotNil(t, req.From())
		assert.Equal(t, "alice", req.From().Address.User)
		require.NotNil(t, req.To())
		assert.Equal(t, "bob", req.To().Address.User)
		require.NotNil(t, req.CallID())
		assert.Equal(t, "compact-call-id", req.CallID().Value())
		require.NotNil(t, req.Contact())
		assert.Equal(t, "alice", req.Contact().Address.User)
		require.NotNil(t, req.ContentType())
		assert.Equal(t, "text/plain", req.ContentType().Value())
		require.NotNil(t, req.ContentLength())
		assert.Equal(t, ContentLengthHeader(0), *req.ContentLength())

		referTo := req.ReferTo()
		require.NotNil(t, referTo)
		assert.Equal(t, "carol", referTo.Address.User)
		referredBy := req.ReferredBy()
		require.NotNil(t, referredBy)
		assert.Equal(t, "alice", referredBy.Address.User)

		sessionExpires := req.SessionExpires()
		require.NotNil(t, sessionExpires)
		assert.Equal(t, uint32(1800), sessionExpires.Delta)
	})

	for _, form := range compactHeaderForms {
		t.Run(form.full, func(t *testing.T) {
			for _, name := range []string{form.full, HeaderToLower(form.full), form.compact, strings.ToUpper(form.compact)} {
				h := req.GetHeader(name)
				require.NotNil(t, h, "GetHeader(%q)", name)
				assert.Len(t, req.GetHeaders(name), 1, "GetHeaders(%q)", name)
			}
		})
	}

	t.Run("NamesKeptOnTheWire", func(t *testing.T) {
		out := req.String()
		for _, line := range []string{"r: <sip:carol@example.com>\r\n", "b: <sip:alice@example.com>\r\n", "o: refer\r\n", "u: refer\r\n", "x: 1800;refresher=uas\r\n"} {
			assert.Contains(t, out, line)
		}
		assert.NotContains(t, out, "Refer-To")
		assert.NotContains(t, out, "Referred-By")
		assert.NotContains(t, out, "Session-Expires")
	})
}

func TestFullHeaderFoundByCompactName(t *testing.T) {
	raw := "NOTIFY sip:alice@10.0.0.1:5060 SIP/2.0\r\n" +
		"Via: SIP/2.0/UDP 10.0.0.2:5060;branch=z9hG4bKfull\r\n" +
		"From: <sip:bob@example.com>;tag=b1\r\n" +
		"To: <sip:alice@example.com>;tag=a1\r\n" +
		"Call-ID: full-call-id\r\n" +
		"CSeq: 3 NOTIFY\r\n" +
		"Event: refer\r\n" +
		"Allow-Events: refer\r\n" +
		"Refer-To: <sip:carol@example.com>\r\n" +
		"Referred-By: <sip:bob@example.com>\r\n" +
		"Session-Expires: 1800;refresher=uac\r\n" +
		"Content-Length: 0\r\n" +
		"\r\n"
	msg, err := ParseMessage([]byte(raw))
	require.NoError(t, err)
	req, ok := msg.(*Request)
	require.True(t, ok)

	for _, name := range []string{"o", "O", "u", "r", "b", "v", "f", "t", "i", "l", "x", "X"} {
		assert.NotNil(t, req.GetHeader(name), "GetHeader(%q)", name)
		assert.Len(t, req.GetHeaders(name), 1, "GetHeaders(%q)", name)
	}
	assert.Nil(t, req.GetHeader("s"))
}

func TestCompactHeaderEditByFullName(t *testing.T) {
	hs := new(headers)
	hs.AppendHeader(NewHeader("o", "refer"))
	hs.AppendHeader(NewHeader("r", "<sip:carol@example.com>"))

	hs.ReplaceHeader(NewHeader("Refer-To", "<sip:dave@example.com>"))
	require.Len(t, hs.Headers(), 2)
	referTo := hs.ReferTo()
	require.NotNil(t, referTo)
	assert.Equal(t, "dave", referTo.Address.User)

	hs.AppendHeaderAfter(NewHeader("Subscription-State", "active"), "Event")
	require.Len(t, hs.Headers(), 3)
	assert.Equal(t, "Subscription-State", hs.Headers()[1].Name())

	assert.True(t, hs.RemoveHeader("Event"))
	assert.Nil(t, hs.GetHeader("o"))
	assert.True(t, hs.RemoveHeader("r"))
	assert.Nil(t, hs.GetHeader("Refer-To"))
}

func TestFullHeaderNameEditsStayCaseSensitive(t *testing.T) {
	hs := new(headers)
	hs.AppendHeader(NewHeader("Event", "refer"))

	// z is no registered compact form, so it is a plain one-letter name.
	hs.AppendHeader(NewHeader("z", "90"))

	assert.False(t, hs.RemoveHeader("event"))
	assert.False(t, hs.RemoveHeader("Z"))
	assert.NotNil(t, hs.GetHeader("event"))
	assert.True(t, hs.RemoveHeader("o"))
	assert.Nil(t, hs.GetHeader("Event"))
}

func TestCompactSerialisationWritesSessionExpiresAsX(t *testing.T) {
	hs := new(headers)
	hs.CompactHeaders = true
	hs.AppendHeader(NewHeader("Session-Expires", "1800;refresher=uac"))
	hs.AppendHeader(NewHeader("Min-SE", "90"))

	assert.Equal(t, "x: 1800;refresher=uac\r\nMin-SE: 90\r\n", hs.String())
}
