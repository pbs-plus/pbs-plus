//go:build linux

package outposttunnel

import (
	"context"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

type addr struct{ hostport string }

func (a addr) Network() string { return "tcp" }
func (a addr) String() string  { return a.hostport }

func remoteAddr(s string) net.Addr {
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return net.TCPAddrFromAddrPort(ap)
	}
	return addr{hostport: s}
}

// streamConn adapts a raw arpc stream to net.Conn so the in-process NFS and S3 servers can serve it.
type streamConn struct {
	arpc.ARPCStream
	local, remote net.Addr
	once          sync.Once
	closed        chan struct{}
}

func newStreamConn(stream arpc.ARPCStream, local, remote net.Addr) *streamConn {
	return &streamConn{ARPCStream: stream, local: local, remote: remote, closed: make(chan struct{})}
}

func (c *streamConn) Close() error {
	var err error
	c.once.Do(func() {
		close(c.closed)
		err = c.ARPCStream.Close()
	})
	return err
}

func (c *streamConn) LocalAddr() net.Addr  { return c.local }
func (c *streamConn) RemoteAddr() net.Addr { return c.remote }

func (c *streamConn) SetReadDeadline(t time.Time) error {
	if d, ok := c.ARPCStream.(interface{ SetReadDeadline(time.Time) error }); ok {
		return d.SetReadDeadline(t)
	}
	return nil
}

func (c *streamConn) SetWriteDeadline(t time.Time) error {
	if d, ok := c.ARPCStream.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return d.SetWriteDeadline(t)
	}
	return nil
}

// listener is the server-side net.Listener of an agent-hosted outpost; connections arrive over the agent tunnel.
type listener struct {
	addr   net.Addr
	conns  chan net.Conn
	ctx    context.Context
	cancel context.CancelFunc
	onStop func()

	mu  sync.Mutex
	err error
}

func newListener(advertised net.Addr, onStop func()) *listener {
	ctx, cancel := context.WithCancel(context.Background())
	return &listener{addr: advertised, conns: make(chan net.Conn), ctx: ctx, cancel: cancel, onStop: onStop}
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	}
}

func (l *listener) Close() error {
	if l.ctx.Err() == nil {
		l.cancel()
		if l.onStop != nil {
			l.onStop()
		}
	}
	return nil
}

func (l *listener) Addr() net.Addr { return l.addr }

func (l *listener) setErr(err error) {
	l.mu.Lock()
	l.err = err
	l.mu.Unlock()
}

func (l *listener) health() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}

// handleConn bridges one forwarded client connection into Accept and holds the raw stream until the conn closes.
func (l *listener) handleConn(remote string) arpc.Response {
	return arpc.Response{Status: arpc.StatusRawStream, RawStream: func(stream arpc.ARPCStream) {
		c := newStreamConn(stream, l.addr, remoteAddr(remote))
		select {
		case l.conns <- c:
		case <-l.ctx.Done():
			_ = c.Close()
			return
		}
		select {
		case <-c.closed:
		case <-l.ctx.Done():
			_ = c.Close()
		}
	}}
}
