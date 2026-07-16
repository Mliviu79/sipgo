package sip

import (
	"net"
	"testing"
)

// These tests pin the two defects that made an inbound INVITE undeliverable on
// a WSS webphone: the pool key derived from the wrong argument, and the missing
// alias registration. Both were silent -- registration and outbound keepalives
// kept working, because those are requests we drive over a socket we opened, so
// nothing ever had to find the connection by the far end's address. Only an
// inbound request needs that lookup, and it simply never arrived.

// testConn is a Connection whose only interesting property is its local address,
// which is the second key the pool derives.
type testConn struct {
	laddr net.Addr
	refs  int
}

func (c *testConn) LocalAddr() net.Addr        { return c.laddr }
func (c *testConn) WriteMsg(msg Message) error { return nil }
func (c *testConn) Ref(i int) int              { c.refs += i; return c.refs }
func (c *testConn) TryClose() (int, error)     { return 0, nil }
func (c *testConn) Close() error               { return nil }

func newTestConn(laddr string) *testConn {
	a, _ := net.ResolveTCPAddr("tcp", laddr)
	return &testConn{laddr: a, refs: 1}
}

// wssRaddr is the shape the vicidial webphone actually produces: dialled by
// hostname for TLS, resolved to an IP that the registrar then uses to address us.
func wssRaddr() Addr {
	return Addr{
		IP:       net.ParseIP("194.102.34.49"),
		Port:     8089,
		Hostname: "ct25.iset.ro",
	}
}

// TestConnectionPoolKeysOnRemoteAddr pins the argument order. The pool keys on
// its FIRST parameter, so passing (laddr, raddr) registers the connection under
// our own local address and under the far end's never -- which is what
// transport_wss.go did, and why an inbound request could not be matched to an
// open socket. UDP, TCP and WS always passed (raddr, laddr); this asserts the
// pool's own contract so a caller cannot quietly disagree with it again.
func TestConnectionPoolKeysOnRemoteAddr(t *testing.T) {
	p := newConnectionPool()
	raddr := Addr{IP: net.ParseIP("194.102.34.49"), Port: 8089}
	laddr := Addr{IP: net.ParseIP("192.168.1.219"), Port: 46336}
	conn := newTestConn("192.168.1.219:46336")

	if _, err := p.addSingleflight(raddr, laddr, true, func() (Connection, error) {
		return conn, nil
	}); err != nil {
		t.Fatalf("addSingleflight() error = %v", err)
	}

	if got := p.getUnref(raddr.String()); got == nil {
		t.Errorf("pool has no entry for raddr %q; an inbound request from the far end cannot find this connection", raddr.String())
	}
	if got := p.getUnref(laddr.String()); got == nil {
		t.Errorf("pool has no entry for the local addr %q", laddr.String())
	}
}

// TestWSConnectionAliasesCoverEveryAddressedForm pins the alias set. The
// registrar may be referenced as hostname:port, bare hostname, IP:port or bare
// IP depending on which header a lookup is driven from, and Addr.String()
// prefers the IP -- so the form we dialled (hostname) is NOT the form we are
// addressed by. Every form must resolve to the one open socket.
func TestWSConnectionAliasesCoverEveryAddressedForm(t *testing.T) {
	aliases := wsConnectionAliases(wssRaddr())

	for _, want := range []string{
		"194.102.34.49:8089", // IP + port: what Addr.String() yields
		"194.102.34.49",      // bare IP
		"ct25.iset.ro:8089",  // hostname + port: what we dialled
		"ct25.iset.ro",       // bare hostname
	} {
		found := false
		for _, a := range aliases {
			if a == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("wsConnectionAliases() missing %q; got %v", want, aliases)
		}
	}
}

// TestWSConnectionAliasesDeduplicates guards the degenerate peers: one with no
// hostname, and one whose hostname IS its IP. Both would otherwise register the
// same key twice. Duplicate keys are harmless in a map but signal the alias set
// is being built by accident rather than by intent.
func TestWSConnectionAliasesDeduplicates(t *testing.T) {
	t.Run("hostname equals ip", func(t *testing.T) {
		aliases := wsConnectionAliases(Addr{
			IP:       net.ParseIP("194.102.34.49"),
			Port:     8089,
			Hostname: "194.102.34.49",
		})
		seen := map[string]int{}
		for _, a := range aliases {
			seen[a]++
		}
		for a, n := range seen {
			if n > 1 {
				t.Errorf("alias %q registered %d times", a, n)
			}
		}
	})

	t.Run("no hostname", func(t *testing.T) {
		aliases := wsConnectionAliases(Addr{IP: net.ParseIP("194.102.34.49"), Port: 8089})
		for _, a := range aliases {
			if a == "" {
				t.Error("wsConnectionAliases() produced an empty alias")
			}
		}
	})
}

// TestAddSingleflightWithAliasesRegistersEveryForm is the end-to-end shape of
// the vicidial regression: dial by hostname, then look the connection up by
// every form the registrar might address it with. Before the fix, only the
// local-address keys existed and all four of these lookups returned nil, so the
// ConfBridge INVITE was never delivered and the ingress blocked forever waiting
// for a call that could not arrive.
func TestAddSingleflightWithAliasesRegistersEveryForm(t *testing.T) {
	p := newConnectionPool()
	raddr := wssRaddr()
	laddr := Addr{IP: net.ParseIP("192.168.1.219"), Port: 46336}
	conn := newTestConn("192.168.1.219:46336")

	if _, err := p.addSingleflightWithAliases(raddr, laddr, true, wsConnectionAliases(raddr), func() (Connection, error) {
		return conn, nil
	}); err != nil {
		t.Fatalf("addSingleflightWithAliases() error = %v", err)
	}

	for _, key := range []string{
		"194.102.34.49:8089",
		"194.102.34.49",
		"ct25.iset.ro:8089",
		"ct25.iset.ro",
		"192.168.1.219:46336", // local addr: still registered, aliases are additive
	} {
		if got := p.getUnref(key); got == nil {
			t.Errorf("pool lookup %q = nil; inbound routing by this form would fail", key)
		}
	}
}

// TestAddSingleflightWithoutAliasesMatchesAddSingleflight pins that the alias
// variant is a strict superset: passing no aliases must behave exactly as the
// original, so every non-WS transport is unaffected by this change.
func TestAddSingleflightWithoutAliasesMatchesAddSingleflight(t *testing.T) {
	raddr := Addr{IP: net.ParseIP("194.102.34.49"), Port: 8089}
	laddr := Addr{IP: net.ParseIP("192.168.1.219"), Port: 46336}

	p := newConnectionPool()
	if _, err := p.addSingleflightWithAliases(raddr, laddr, true, nil, func() (Connection, error) {
		return newTestConn("192.168.1.219:46336"), nil
	}); err != nil {
		t.Fatalf("addSingleflightWithAliases() error = %v", err)
	}

	if p.Size() != 2 {
		t.Errorf("pool size = %d, want 2 (raddr + local addr); aliases must not add keys when none are passed", p.Size())
	}
}
