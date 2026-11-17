package sip

import (
	"net"
	"sync"
)

// onceCloseListener is a net.Listener whose Close runs the wrapped listener's
// Close once and returns that first result on every call.
type onceCloseListener struct {
	net.Listener
	closeOnce func() error
}

func (l *onceCloseListener) Close() error { return l.closeOnce() }

// NewOnceCloseListener returns l with a Close that closes it once and returns
// the first result on every call, so the transport layer and a ctx watcher can
// share one close. A listener already wrapped is returned as it is.
func NewOnceCloseListener(l net.Listener) net.Listener {
	if w, ok := l.(*onceCloseListener); ok {
		return w
	}
	return &onceCloseListener{Listener: l, closeOnce: sync.OnceValue(l.Close)}
}

// onceClosePacketConn is the net.PacketConn counterpart of onceCloseListener.
type onceClosePacketConn struct {
	net.PacketConn
	closeOnce func() error
}

func (c *onceClosePacketConn) Close() error { return c.closeOnce() }

// NewOnceClosePacketConn returns c with a Close that closes it once and returns
// the first result on every call. A conn already wrapped is returned as it is.
func NewOnceClosePacketConn(c net.PacketConn) net.PacketConn {
	if w, ok := c.(*onceClosePacketConn); ok {
		return w
	}
	return &onceClosePacketConn{PacketConn: c, closeOnce: sync.OnceValue(c.Close)}
}
