package sip

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTransportLayerCloseClosesServedListenersOnce(t *testing.T) {
	tpl := NewTransportLayer(net.DefaultResolver, NewParser(), nil)

	udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	udpDone := make(chan error, 1)
	tcpDone := make(chan error, 1)
	go func() { udpDone <- tpl.ServeUDP(udp) }()
	go func() { tcpDone <- tpl.ServeTCP(tcp) }()
	require.Eventually(t, func() bool {
		return len(tpl.ListenPorts("udp")) > 0 && len(tpl.ListenPorts("tcp")) > 0
	}, 5*time.Second, time.Millisecond)

	require.NoError(t, tpl.Close())
	require.NoError(t, <-udpDone)
	require.ErrorIs(t, <-tcpDone, net.ErrClosed)

	// Closing again closes nothing twice.
	require.NoError(t, tpl.Close())
}

func TestTransportLayerServeAfterCloseClosesTheListener(t *testing.T) {
	tpl := NewTransportLayer(net.DefaultResolver, NewParser(), nil)
	require.NoError(t, tpl.Close())

	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	require.ErrorIs(t, tpl.ServeTCP(tcp), net.ErrClosed)

	_, err = tcp.Accept()
	require.ErrorIs(t, err, net.ErrClosed, "the listener served after Close must be closed")
}
