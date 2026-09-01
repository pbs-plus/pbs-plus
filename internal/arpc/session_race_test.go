package arpc

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
	"time"
)

func waitUntil(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestStreamReconnectDoesNotEvictNewSession(t *testing.T) {
	router := NewRouter()
	addr, shutdown, _, agentsManager := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)

	first, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("first connect: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return agentsManager.IsOnline("client") }, "first session never registered")

	old, _ := agentsManager.GetStreamPipe("client")

	second, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool {
		cur, ok := agentsManager.GetStreamPipe("client")
		return ok && cur != old
	}, "reconnect never replaced the registration")

	time.Sleep(200 * time.Millisecond)

	if !agentsManager.IsOnline("client") {
		t.Fatal("old owner's exit evicted the replacement session; the agent is " +
			"connected but the server reports it offline")
	}

	_ = first
	_ = second
}

func TestQuicReconnectDoesNotEvictNewSession(t *testing.T) {
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
	defer func() { _ = CloseQuicTransport(transport) }()

	listener, err := ListenQuic(transport, serverTLS)
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	agentsManager := NewAgentsManager()
	agentsManager.SetExtraExpectFunc(func(string, []*x509.Certificate) bool { return true })
	go func() { _ = ServeQuic(t.Context(), agentsManager, listener, NewRouter()) }()

	clientTLS := newTestClientTLS(t)
	addr := listener.Addr().String()

	first, err := DialQuic(t.Context(), addr, clientTLS, nil)
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}
	waitUntil(t, 2*time.Second, func() bool { return agentsManager.IsOnline("client") }, "first session never registered")

	second, err := DialQuic(t.Context(), addr, clientTLS, nil)
	if err != nil {
		t.Fatalf("reconnect dial: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	if !agentsManager.IsOnline("client") {
		t.Fatal("old owner's exit evicted the replacement session; the agent is " +
			"connected but the server reports it offline")
	}

	_ = first
	_ = second
}
