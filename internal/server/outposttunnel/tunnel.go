//go:build linux

// Package outposttunnel places outpost listeners on agents: the agent accepts client TCP connections and
// forwards each one over its server connection, while the protocol server keeps running in this process.
package outposttunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/outpostwire"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
)

const (
	callTimeout = 30 * time.Second
	minBackoff  = time.Second
	maxBackoff  = 30 * time.Second
	stableAfter = time.Minute
)

type caller interface {
	CallMessage(ctx context.Context, method string, payload any) (string, error)
}

type childPipe interface {
	caller
	SetRouter(arpc.Router)
	CloseChan() <-chan struct{}
	Close()
}

type registry interface {
	Expect(id string)
	NotExpect(id string)
	session(agent string) (caller, bool)
	waitChild(ctx context.Context, id string) (childPipe, error)
}

type managerRegistry struct{ *arpc.AgentsManager }

func (m managerRegistry) session(agent string) (caller, bool) {
	if q, ok := m.GetQuicPipe(agent); ok {
		return q, true
	}
	if s, ok := m.GetStreamPipe(agent); ok {
		return s, true
	}
	return nil, false
}

func (m managerRegistry) waitChild(ctx context.Context, id string) (childPipe, error) {
	return m.WaitStreamPipe(ctx, id)
}

// Tunnel is the outpost.Placement for outposts hosted on agents.
type Tunnel struct {
	agents registry

	mu        sync.Mutex
	listeners map[string]*listener
}

func New(agents *arpc.AgentsManager) *Tunnel { return newTunnel(managerRegistry{agents}) }

func newTunnel(agents registry) *Tunnel {
	return &Tunnel{agents: agents, listeners: map[string]*listener{}}
}

func advertisedAddr(o outpost.Outpost) (net.Addr, error) {
	host, port, err := net.SplitHostPort(o.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid listen_addr: %w", err)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = o.Agent
	}
	return addr{hostport: net.JoinHostPort(host, port)}, nil
}

func (t *Tunnel) Listen(_ context.Context, o outpost.Outpost) (net.Listener, error) {
	advertised, err := advertisedAddr(o)
	if err != nil {
		return nil, err
	}
	var l *listener
	l = newListener(advertised, func() { t.forget(o.Name, l) })
	l.setErr(fmt.Errorf("waiting for agent %s", o.Agent))
	t.mu.Lock()
	previous := t.listeners[o.Name]
	t.listeners[o.Name] = l
	t.mu.Unlock()
	if previous != nil {
		_ = previous.Close()
	}
	go t.supervise(l, o)
	return l, nil
}

func (t *Tunnel) Health(o outpost.Outpost) error {
	t.mu.Lock()
	l := t.listeners[o.Name]
	t.mu.Unlock()
	if l == nil {
		return nil
	}
	return l.health()
}

func (t *Tunnel) forget(name string, l *listener) {
	t.mu.Lock()
	if t.listeners[name] == l {
		delete(t.listeners, name)
	}
	t.mu.Unlock()
}

func (t *Tunnel) supervise(l *listener, o outpost.Outpost) {
	id := outpostwire.StreamID(o.Agent, o.Name)
	t.agents.Expect(id)
	defer t.agents.NotExpect(id)
	defer t.stopOnAgent(o)

	backoff := minBackoff
	for {
		started := time.Now()
		err := t.runOnce(l, o, id)
		if l.ctx.Err() != nil {
			return
		}
		l.setErr(err)
		log.Warn("outpost tunnel down", "outpost", o.Name, "agent", o.Agent, "error", err.Error())
		if time.Since(started) > stableAfter {
			backoff = minBackoff
		}
		select {
		case <-l.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func (t *Tunnel) runOnce(l *listener, o outpost.Outpost, id string) error {
	sess, ok := t.agents.session(o.Agent)
	if !ok {
		return fmt.Errorf("agent %s is not connected", o.Agent)
	}
	ctx, cancel := context.WithTimeout(l.ctx, callTimeout)
	defer cancel()
	if _, err := sess.CallMessage(ctx, outpostwire.MethodStart, &outpostwire.StartReq{Name: o.Name, ListenAddr: o.ListenAddr}); err != nil {
		return fmt.Errorf("agent %s could not start the outpost: %w", o.Agent, err)
	}
	pipe, err := t.agents.waitChild(ctx, id)
	if err != nil {
		return fmt.Errorf("agent %s did not connect the outpost tunnel: %w", o.Agent, err)
	}
	router := arpc.NewRouter()
	router.Handle(outpostwire.MethodConn, func(req *arpc.Request) (arpc.Response, error) {
		var in outpostwire.ConnReq
		if err := cbor.Unmarshal(req.Payload, &in); err != nil {
			return arpc.Response{}, err
		}
		return l.handleConn(in.Remote), nil
	})
	pipe.SetRouter(router)
	if _, err := pipe.CallMessage(ctx, outpostwire.MethodReady, nil); err != nil {
		pipe.Close()
		return fmt.Errorf("outpost tunnel to agent %s failed: %w", o.Agent, err)
	}
	l.setErr(nil)
	log.Info("outpost tunnel up", "outpost", o.Name, "agent", o.Agent)
	select {
	case <-pipe.CloseChan():
		return errors.New("outpost tunnel to agent " + o.Agent + " closed")
	case <-l.ctx.Done():
		pipe.Close()
		return nil
	}
}

func (t *Tunnel) stopOnAgent(o outpost.Outpost) {
	sess, ok := t.agents.session(o.Agent)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sess.CallMessage(ctx, outpostwire.MethodStop, &outpostwire.StopReq{Name: o.Name}); err != nil {
		log.Warn("stopping outpost on agent", "outpost", o.Name, "agent", o.Agent, "error", err.Error())
	}
}
