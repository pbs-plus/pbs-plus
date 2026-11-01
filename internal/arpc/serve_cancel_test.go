package arpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

func newIdlePipe(t *testing.T) (*StreamPipe, context.CancelFunc, net.Conn) {
	t.Helper()

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	clientSess, err := smux.Client(clientConn, nil)
	if err != nil {
		t.Fatalf("smux client: %v", err)
	}
	t.Cleanup(func() { _ = clientSess.Close() })

	serverSess, err := smux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("smux server: %v", err)
	}
	t.Cleanup(func() { _ = serverSess.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	pipe, err := newStreamPipe(ctx, serverSess, serverConn, "", nil)
	if err != nil {
		cancel()
		t.Fatalf("newStreamPipe: %v", err)
	}
	return pipe, cancel, serverConn
}

func TestServeReturnsWhenContextIsCancelledOnALiveTunnel(t *testing.T) {
	pipe, cancel, _ := newIdlePipe(t)
	defer cancel()

	served := make(chan error, 1)
	go func() { served <- pipe.Serve() }()

	cancel()

	select {
	case err := <-served:
		if err == nil {
			t.Fatal("Serve returned nil, want the closed-tunnel error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of context cancellation on a live tunnel")
	}
}

func TestServeReturnsOnTunnelClosureWithoutCancellation(t *testing.T) {
	pipe, cancel, conn := newIdlePipe(t)
	defer cancel()

	served := make(chan error, 1)
	go func() { served <- pipe.Serve() }()

	if err := conn.Close(); err != nil {
		t.Fatalf("closing the underlying conn: %v", err)
	}

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of tunnel closure")
	}
}
