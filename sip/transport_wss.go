package sip

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"time"
)

// TLS transport implementation
type TransportWSS struct {
	*TransportWS
}

func (t *TransportWSS) init(par *Parser, dialTLSConf *tls.Config) {
	// Set before TransportWS.init, which otherwise defaults this to the ws:// scheme.
	if t.DialURI == nil {
		t.DialURI = func(addr string) string { return "wss://" + addr }
	}
	t.TransportWS.init(par)
	t.TransportWS.transport = "WSS"
	t.dialer.TLSConfig = dialTLSConf

	t.dialer.TLSClient = func(conn net.Conn, hostname string) net.Conn {
		// This is just extracted from tls dialer code
		config := dialTLSConf

		if config.ServerName == "" {
			config = config.Clone()
			config.ServerName = hostname
		}
		return tls.Client(conn, config)
	}

	if t.log == nil {
		t.log = DefaultLogger()
	}
}

func (t *TransportWSS) String() string {
	return "transport<WSS>"
}

// wsConnectionAliases returns the address forms a WebSocket connection to raddr
// must also be findable under, beyond the one the pool derives from raddr itself.
//
// A WebSocket to a registrar is dialled by hostname, because TLS verifies the
// certificate against that name, but the registrar identifies itself by IP in
// the SIP traffic it sends back -- and Addr.String() prefers a resolved IP, so
// the dialled form and the addressed form are routinely different strings for
// the same peer. Either form may also appear with or without the port, depending
// on the header a lookup is driven from.
//
// Registering every form is what keeps a lookup by any of them resolving to the
// one socket that is actually open. It matters more here than on other
// transports because a WS client is never reachable at its contact address: the
// connection it opened is the only path back to it, so failing to match it is
// not a slow path, it is an undeliverable request.
//
// Duplicates are dropped, so a peer with no hostname -- or one whose hostname is
// its IP -- does not register the same key twice.
func wsConnectionAliases(raddr Addr) []string {
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		for _, existing := range out {
			if existing == s {
				return
			}
		}
		out = append(out, s)
	}

	port := strconv.Itoa(raddr.Port)
	if raddr.IP != nil {
		add(net.JoinHostPort(raddr.IP.String(), port))
		add(raddr.IP.String())
	}
	if raddr.Hostname != "" {
		add(net.JoinHostPort(raddr.Hostname, port))
		add(raddr.Hostname)
	}
	return out
}

// CreateConnection creates WSS connection for TCP transport
// TODO Make this consisten with TCP
func (t *TransportWSS) CreateConnection(ctx context.Context, laddr Addr, raddr Addr, handler MessageHandler) (Connection, error) {
	log := t.log

	// Must have IP resolved
	if raddr.IP == nil {
		return nil, fmt.Errorf("remote address IP not resolved")
	}

	// raddr first. The pool keys on its first argument, so passing laddr there
	// registered this socket under our own local address -- twice, since the pool
	// also keys the local address -- and under the registrar's address never,
	// leaving every lookup the far end drives with no connection to find. UDP, TCP
	// and WS all pass this order; WSS alone had it reversed.
	//
	// The aliases cover the address forms other than the dialled one; see
	// wsConnectionAliases.
	conn, err := t.pool.addSingleflightWithAliases(raddr, laddr, t.connectionReuse, wsConnectionAliases(raddr), func() (Connection, error) {
		// We need to distict IPAddr vs address with hostname
		// Hostname must be passed for TLS if provided due to certificates check
		hostname := raddr.Hostname
		if hostname == "" {
			hostname = raddr.IP.String()
		}
		addr := net.JoinHostPort(hostname, strconv.Itoa(raddr.Port))

		// USe default unless local address is set
		var tladdr *net.TCPAddr = nil
		if laddr.IP != nil {
			tladdr = &net.TCPAddr{
				IP:   laddr.IP,
				Port: laddr.Port,
			}
		}

		traddr := &net.TCPAddr{
			IP:   raddr.IP,
			Port: raddr.Port,
		}

		// Make sure we have port set
		if traddr.Port == 0 {
			traddr.Port = 443
		}

		netDialer := &net.Dialer{
			LocalAddr: tladdr,
		}

		log.Debug("Dialing new connection", "raddr", traddr.String())
		conn, err := netDialer.DialContext(ctx, "tcp", traddr.String())
		if err != nil {
			return nil, fmt.Errorf("dial TCP error: %w", err)
		}

		log.Debug("Setuping TLS connection", "hostname", hostname)
		tlsConn := t.dialer.TLSClient(conn, hostname)

		u, err := url.ParseRequestURI(t.DialURI(addr))
		if err != nil {
			return nil, fmt.Errorf("parse request wss uri failed: %w", err)
		}

		// Check ctx deadline
		// TODO handle cancelation?
		if deadline, ok := ctx.Deadline(); ok {
			tlsConn.SetDeadline(deadline)
			defer tlsConn.SetDeadline(time.Time{})
		}

		_, _, err = t.dialer.Upgrade(tlsConn, u)
		if err != nil {
			return nil, fmt.Errorf("failed to upgrade: %w", err)
		}

		c := newWSConnection(tlsConn, true, 2+TransportIdleConnection)
		go t.readConnection(c, c.LocalAddr().String(), c.RemoteAddr().String(), handler)
		go c.keepalive(t.log)
		return c, nil
	})
	if err != nil {
		return nil, err
	}
	c := conn.(*WSConnection)
	return c, nil
}
