package arpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"testing"
	"time"
)

func newLeakTestQuicPair(t *testing.T, agentRouter Router) *QuicPipe {
	t.Helper()
	ensureGlobalCAs(t)
	serverCert := serverCA.issueCert(t, "localhost", false, []net.IP{net.ParseIP("127.0.0.1")}, []string{"localhost"})
	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA.caPool,
		MinVersion:   tls.VersionTLS13,
	}

	transport, err := NewQuicTransport("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("quic transport: %v", err)
	}
	t.Cleanup(func() { _ = CloseQuicTransport(transport) })

	listener, err := ListenQuic(transport, serverTLS)
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	agentsManager := NewAgentsManager()
	agentsManager.SetExtraExpectFunc(func(string, []*x509.Certificate) bool { return true })
	go func() { _ = ServeQuic(t.Context(), agentsManager, listener, NewRouter()) }()

	headers := http.Header{}
	headers.Set("X-PBS-Agent", "client")

	agent, err := DialQuic(t.Context(), listener.Addr().String(), newTestClientTLS(t), headers)
	if err != nil {
		t.Fatalf("agent dial: %v", err)
	}
	agent.SetRouter(agentRouter)
	go func() { _ = agent.Serve() }()

	waitUntil(t, 5*time.Second, func() bool { return agentsManager.IsOnline("client") }, "agent session never registered")

	pipe, ok := agentsManager.GetQuicPipe("client")
	if !ok {
		t.Fatal("quic pipe not registered")
	}
	return pipe
}

func leakTestRouter() Router {
	router := NewRouter()
	router.Handle("slow", func(req *Request) (Response, error) {
		select {
		case <-time.After(300 * time.Millisecond):
		case <-req.Context.Done():
		}
		return Response{Status: 200, Message: "slow done"}, nil
	})
	router.Handle("ping", func(*Request) (Response, error) {
		return Response{Status: 200, Message: "pong"}, nil
	})
	return router
}

func callsBeyondStreamLimit() int {
	return int(quicConfig().MaxIncomingStreams) + 8
}

// Timed-out status probes must not wedge the later backup call.
func TestQuicCallReleasesStreamAfterTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("binds UDP sockets")
	}
	pipe := newLeakTestQuicPair(t, leakTestRouter())

	for i := range callsBeyondStreamLimit() {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		_, err := pipe.CallMessage(ctx, "slow", nil)
		cancel()
		if err == nil {
			t.Fatalf("call %d unexpectedly succeeded; the probe was meant to time out", i)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msg, err := pipe.CallMessage(ctx, "ping", nil)
	if err != nil {
		t.Fatalf("agent stopped answering after %d timed-out calls: %v", callsBeyondStreamLimit(), err)
	}
	if msg != "pong" {
		t.Fatalf("reply = %q, want %q", msg, "pong")
	}
}

// Successful calls must release their stream too.
func TestQuicCallReleasesStreamAfterSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("binds UDP sockets")
	}
	pipe := newLeakTestQuicPair(t, leakTestRouter())

	for i := range callsBeyondStreamLimit() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		msg, err := pipe.CallMessage(ctx, "ping", nil)
		cancel()
		if err != nil {
			t.Fatalf("call %d failed after %d successful calls: %v", i, i, err)
		}
		if msg != "pong" {
			t.Fatalf("call %d reply = %q, want %q", i, msg, "pong")
		}
	}
}
