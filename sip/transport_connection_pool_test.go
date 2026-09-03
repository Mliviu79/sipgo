package sip

import (
	"errors"
	"net"
	"sync"
	"testing"

	"github.com/emiago/sipgo/fakes"
)

func TestConnectionPool(t *testing.T) {
	pool := newConnectionPool()

	fakeConn := &fakes.TCPConn{
		LAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
		RAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5060},
		Reader: nil,
		Writer: nil,
	}
	conn := &TCPConnection{Conn: fakeConn}

	pool.Add(fakeConn.RAddr.String(), conn)

	c := pool.Get(fakeConn.RAddr.String())
	if c != conn {
		t.Fatal("Not found connection")
	}
}

func BenchmarkConnectionPool(b *testing.B) {
	pool := newConnectionPool()

	for i := 0; i < b.N; i++ {
		conn := &TCPConnection{Conn: &fakes.TCPConn{
			LAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 5060},
			RAddr:  net.TCPAddr{IP: net.ParseIP("127.0.0.2"), Port: 5060},
			Reader: nil,
			Writer: nil,
		}}
		a := &net.TCPAddr{
			IP:   net.IPv4('1', '2', '3', byte(i)),
			Port: 1000,
		}
		pool.Add(a.String(), conn)
		c := pool.Get(a.String())
		if c != conn {
			b.Fatal("mismatched function")
		}
	}
}

// closedPoolConn records whether the pool closed it, which is the only
// observable difference between a connection that was registered and one the
// pool refused after its transport went away.
type closedPoolConn struct {
	laddr  net.Addr
	mu     sync.Mutex
	refs   int
	closes int
}

func (c *closedPoolConn) LocalAddr() net.Addr        { return c.laddr }
func (c *closedPoolConn) WriteMsg(msg Message) error { return nil }

func (c *closedPoolConn) Ref(i int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refs += i
	return c.refs
}

func (c *closedPoolConn) TryClose() (int, error) { return 0, nil }

func (c *closedPoolConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closes++
	return nil
}

func (c *closedPoolConn) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closes
}

func newClosedPoolConn(laddr string) *closedPoolConn {
	a, _ := net.ResolveTCPAddr("tcp", laddr)
	return &closedPoolConn{laddr: a, refs: 1}
}

// TestConnectionPoolRefusesAfterClear pins the create-after-close guard. Clear
// runs when the owning transport closes, and nothing clears the pool a second
// time -- so a connection registered after it is unreachable by any reaper and
// its reader goroutine stays parked on an open socket for the life of the
// process. A client transaction still retransmitting when the transport closes
// reaches exactly this path, which is why the refusal is before the dial rather
// than after it: the socket must never be opened.
func TestConnectionPoolRefusesAfterClear(t *testing.T) {
	p := newConnectionPool()
	if err := p.Clear(); err != nil {
		t.Fatalf("Clear() error = %v", err)
	}

	raddr := Addr{IP: net.ParseIP("127.0.0.1"), Port: 5060}
	laddr := Addr{IP: net.ParseIP("127.0.0.1"), Port: 46336}

	dialled := 0
	c, err := p.addSingleflight(raddr, laddr, true, func() (Connection, error) {
		dialled++
		return newClosedPoolConn("127.0.0.1:46336"), nil
	})

	if !errors.Is(err, errPoolClosed) {
		t.Errorf("addSingleflight() error = %v, want one wrapping errPoolClosed", err)
	}
	if c != nil {
		t.Errorf("addSingleflight() connection = %v, want nil", c)
	}
	if dialled != 0 {
		t.Errorf("dial ran %d times, want 0; a pool whose transport is closed must not open a socket", dialled)
	}
	if p.Size() != 0 {
		t.Errorf("pool size = %d, want 0", p.Size())
	}
}

// TestConnectionPoolClosesAConnectionDialledAcrossClear covers the window the
// early refusal cannot: the transport closes while a dial is already in flight.
// The connection exists by then, so refusing without closing it would leak the
// very socket the guard exists to prevent. Both arms are exercised because the
// pool has two register sites and each needed its own check.
func TestConnectionPoolClosesAConnectionDialledAcrossClear(t *testing.T) {
	raddr := Addr{IP: net.ParseIP("127.0.0.1"), Port: 5060}

	for _, tc := range []struct {
		name  string
		laddr Addr
		reuse bool
	}{
		// laddr.Port > 0 takes the singleflight path.
		{name: "singleflight path", laddr: Addr{IP: net.ParseIP("127.0.0.1"), Port: 46336}, reuse: false},
		// A zero local port with no reuse takes the unblocked path.
		{name: "unblocked path", laddr: Addr{IP: net.ParseIP("127.0.0.1")}, reuse: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newConnectionPool()
			conn := newClosedPoolConn("127.0.0.1:46336")

			c, err := p.addSingleflight(raddr, tc.laddr, tc.reuse, func() (Connection, error) {
				// The transport closes mid-dial.
				if err := p.Clear(); err != nil {
					t.Fatalf("Clear() error = %v", err)
				}
				return conn, nil
			})

			if !errors.Is(err, errPoolClosed) {
				t.Errorf("addSingleflight() error = %v, want one wrapping errPoolClosed", err)
			}
			if c != nil {
				t.Errorf("addSingleflight() connection = %v, want nil", c)
			}
			if got := conn.closeCount(); got != 1 {
				t.Errorf("connection closed %d times, want 1; a connection the pool refuses must not be left open", got)
			}
			if p.Size() != 0 {
				t.Errorf("pool size = %d, want 0; the refused connection must not be registered", p.Size())
			}
		})
	}
}
