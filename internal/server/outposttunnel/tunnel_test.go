//go:build linux

package outposttunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/outpostwire"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
)

type fakeCaller struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (c *fakeCaller) CallMessage(_ context.Context, method string, _ any) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, method)
	return "ok", c.err
}

func (c *fakeCaller) called() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

type fakeChild struct {
	fakeCaller
	routed chan struct{}
	closed chan struct{}
	once   sync.Once
}

func newFakeChild() *fakeChild {
	return &fakeChild{routed: make(chan struct{}, 1), closed: make(chan struct{})}
}

func (c *fakeChild) SetRouter(arpc.Router)      { c.routed <- struct{}{} }
func (c *fakeChild) CloseChan() <-chan struct{} { return c.closed }
func (c *fakeChild) Close()                     { c.once.Do(func() { close(c.closed) }) }

type fakeRegistry struct {
	mu       sync.Mutex
	expected map[string]bool
	main     *fakeCaller
	online   bool
	children chan *fakeChild
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{expected: map[string]bool{}, main: &fakeCaller{}, online: true, children: make(chan *fakeChild, 4)}
}

func (r *fakeRegistry) Expect(id string)    { r.mu.Lock(); r.expected[id] = true; r.mu.Unlock() }
func (r *fakeRegistry) NotExpect(id string) { r.mu.Lock(); delete(r.expected, id); r.mu.Unlock() }

func (r *fakeRegistry) isExpected(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.expected[id]
}

func (r *fakeRegistry) session(string) (caller, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.main, r.online
}

func (r *fakeRegistry) waitChild(ctx context.Context, id string) (childPipe, error) {
	if !r.isExpected(id) {
		return nil, errors.New("child not expected")
	}
	select {
	case c := <-r.children:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testOutpost() outpost.Outpost {
	return outpost.Outpost{Name: "edge", Type: outpost.TypeNFS, Agent: "web01.lan", ListenAddr: "0.0.0.0:2049"}
}

func TestAdvertisedAddr(t *testing.T) {
	for listen, want := range map[string]string{
		"0.0.0.0:2049":  "web01.lan:2049",
		":2049":         "web01.lan:2049",
		"[::]:9100":     "web01.lan:9100",
		"10.0.0.7:2049": "10.0.0.7:2049",
	} {
		o := testOutpost()
		o.ListenAddr = listen
		got, err := advertisedAddr(o)
		if err != nil || got.String() != want {
			t.Errorf("advertisedAddr(%q) = %v, %v; want %s", listen, got, err, want)
		}
	}
}

func TestTunnelLifecycle(t *testing.T) {
	reg := newFakeRegistry()
	tun := newTunnel(reg)
	o := testOutpost()
	id := outpostwire.StreamID(o.Agent, o.Name)

	reg.mu.Lock()
	reg.online = false
	reg.mu.Unlock()
	ln, err := tun.Listen(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	if ln.Addr().String() != "web01.lan:2049" {
		t.Fatalf("addr = %s", ln.Addr())
	}
	eventually(t, "offline health", func() bool {
		err := tun.Health(o)
		return err != nil && strings.Contains(err.Error(), "not connected")
	})
	if !reg.isExpected(id) {
		t.Fatal("child connection is not expected while the outpost runs")
	}

	reg.mu.Lock()
	reg.online = true
	reg.mu.Unlock()
	child := newFakeChild()
	reg.children <- child
	<-child.routed
	eventually(t, "healthy tunnel", func() bool { return tun.Health(o) == nil })
	if got := strings.Join(reg.main.called(), ","); got != outpostwire.MethodStart {
		t.Fatalf("main session calls = %s", got)
	}
	if got := strings.Join(child.called(), ","); got != outpostwire.MethodReady {
		t.Fatalf("child calls = %s", got)
	}

	child.Close()
	eventually(t, "closed tunnel reported", func() bool { return tun.Health(o) != nil })
	second := newFakeChild()
	reg.children <- second
	<-second.routed
	eventually(t, "reconnected tunnel", func() bool { return tun.Health(o) == nil })

	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, "stop sent to agent", func() bool {
		calls := reg.main.called()
		return calls[len(calls)-1] == outpostwire.MethodStop
	})
	eventually(t, "expectation dropped", func() bool { return !reg.isExpected(id) })
	select {
	case <-second.closed:
	default:
		t.Fatal("tunnel pipe left open after the listener closed")
	}
	if tun.Health(o) != nil {
		t.Fatal("stopped outpost still reports health")
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after close = %v", err)
	}
}

func TestStartFailureIsReported(t *testing.T) {
	reg := newFakeRegistry()
	reg.main.err = errors.New("could not listen on 0.0.0.0:2049")
	tun := newTunnel(reg)
	o := testOutpost()
	ln, err := tun.Listen(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	eventually(t, "start error in health", func() bool {
		err := tun.Health(o)
		return err != nil && strings.Contains(err.Error(), "could not listen")
	})
}

func TestForwardedConnBridgesBothWays(t *testing.T) {
	l := newListener(addr{hostport: "web01.lan:2049"}, nil)
	t.Cleanup(func() { _ = l.Close() })
	agentSide, serverSide := net.Pipe()
	resp := l.handleConn("192.0.2.10:51000")
	if resp.Status != arpc.StatusRawStream || resp.RawStream == nil {
		t.Fatalf("response = %+v", resp)
	}
	done := make(chan struct{})
	go func() {
		resp.RawStream(serverSide)
		close(done)
	}()

	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if conn.RemoteAddr().String() != "192.0.2.10:51000" || conn.LocalAddr().String() != "web01.lan:2049" {
		t.Fatalf("addrs = %s -> %s", conn.RemoteAddr(), conn.LocalAddr())
	}
	go func() { _, _ = agentSide.Write([]byte("ping")) }()
	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("read = %q, %v", buf, err)
	}
	go func() { _, _ = conn.Write([]byte("pong")) }()
	if _, err := io.ReadFull(agentSide, buf); err != nil || string(buf) != "pong" {
		t.Fatalf("reply = %q, %v", buf, err)
	}

	select {
	case <-done:
		t.Fatal("raw stream released while the conn is open")
	case <-time.After(50 * time.Millisecond):
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("raw stream not released after the conn closed")
	}
}
