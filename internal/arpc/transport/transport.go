// package arpc/transport

package transport

import (
	"context"
	"net"
	"net/netip"
)

// Transport is the abstraction for network connectivity
type Transport interface {
	// DialContext establishes an outgoing connection
	DialContext(ctx context.Context, network, address string) (net.Conn, error)

	// Listen creates a listener for incoming connections
	Listen(network, address string) (net.Listener, error)

	// Close shuts down the transport
	Close() error

	// LocalAddr returns the local address of this transport
	LocalAddr() netip.Addr
}
