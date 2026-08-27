package arpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

func TestStatelessResetKeyIsStableAcrossRestarts(t *testing.T) {
	secret := []byte("server private key material")

	first, err := statelessResetKey(secret)
	if err != nil {
		t.Fatalf("derive first key: %v", err)
	}
	second, err := statelessResetKey(secret)
	if err != nil {
		t.Fatalf("derive second key: %v", err)
	}

	if !bytes.Equal(first[:], second[:]) {
		t.Fatal("same secret produced different keys; a restarted server " +
			"would not reset agents holding dead connections")
	}

	other, err := statelessResetKey([]byte("a different server"))
	if err != nil {
		t.Fatalf("derive other key: %v", err)
	}
	if bytes.Equal(first[:], other[:]) {
		t.Fatal("different secrets produced the same key")
	}

	var zero [32]byte
	if bytes.Equal(first[:], zero[:]) {
		t.Fatal("derived key is all zeroes")
	}
}

func TestNewQuicTransportWithoutSecret(t *testing.T) {
	transport, err := NewQuicTransport("127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("bind transport: %v", err)
	}
	defer func() { _ = CloseQuicTransport(transport) }()

	if transport.StatelessResetKey != nil {
		t.Fatal("expected no stateless reset key without a secret")
	}
}

func TestNewQuicTransportSetsResetKey(t *testing.T) {
	transport, err := NewQuicTransport("127.0.0.1:0", []byte("secret"))
	if err != nil {
		t.Fatalf("bind transport: %v", err)
	}
	defer func() { _ = CloseQuicTransport(transport) }()

	if transport.StatelessResetKey == nil {
		t.Fatal("expected a stateless reset key to be configured")
	}
}

// The two settings compose, so neither one alone bounds agent recovery.
func TestQuicIdleTimeoutBoundsAgentRecovery(t *testing.T) {
	cfg := quicConfig()

	if cfg.KeepAlivePeriod <= 0 {
		t.Fatal("keepalive disabled; healthy idle sessions would be dropped")
	}
	if cfg.KeepAlivePeriod*2 >= cfg.MaxIdleTimeout {
		t.Fatalf("keepalive %v leaves no margin under idle timeout %v; "+
			"healthy sessions would be torn down",
			cfg.KeepAlivePeriod, cfg.MaxIdleTimeout)
	}

	worstCase := cfg.KeepAlivePeriod + cfg.MaxIdleTimeout
	if worstCase > 90*time.Second {
		t.Fatalf("an idle agent needs up to %v (keepalive %v + idle %v) to "+
			"drop a dead session and become usable again",
			worstCase, cfg.KeepAlivePeriod, cfg.MaxIdleTimeout)
	}
}

func newQuicServerTLS(t *testing.T) *tls.Config {
	t.Helper()
	ensureGlobalCAs(t)
	serverCert := serverCA.issueCert(
		t, "localhost", false, []net.IP{net.ParseIP("127.0.0.1")},
		[]string{"localhost"},
	)

	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    clientCA.caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   quicNextProtos,
	}
}

// Proves a restarted server resets an agent holding a dead session.
func TestAgentRecoversFromServerRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("binds UDP sockets and restarts a listener")
	}

	serverTLS := newQuicServerTLS(t)
	secret := []byte("stable server key material")

	transport, err := NewQuicTransport("127.0.0.1:0", secret)
	if err != nil {
		t.Fatalf("bind transport: %v", err)
	}
	addr := transport.Conn.LocalAddr().String()

	listener, err := ListenQuic(transport, serverTLS)
	if err != nil {
		_ = CloseQuicTransport(transport)
		t.Fatalf("listen: %v", err)
	}

	accepted := make(chan struct{}, 1)
	go func() {
		if _, aerr := listener.Accept(context.Background()); aerr == nil {
			accepted <- struct{}{}
		}
	}()

	clientTLS := newTestClientTLS(t)
	clientTLS.NextProtos = quicNextProtos

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelDial()

	clientConn, err := quic.DialAddr(dialCtx, addr, clientTLS, quicConfig())
	if err != nil {
		_ = listener.Close()
		_ = CloseQuicTransport(transport)
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = clientConn.CloseWithError(0, "done") }()

	select {
	case <-accepted:
	case <-time.After(15 * time.Second):
		_ = listener.Close()
		_ = CloseQuicTransport(transport)
		t.Fatal("server never accepted the agent session")
	}

	if clientConn.Context().Err() != nil {
		t.Fatal("agent session died before the restart")
	}

	_ = listener.Close()
	if cerr := CloseQuicTransport(transport); cerr != nil {
		t.Fatalf("close transport: %v", cerr)
	}

	restarted, err := NewQuicTransport(addr, secret)
	if err != nil {
		t.Fatalf("rebind transport on %s: %v", addr, err)
	}
	defer func() { _ = CloseQuicTransport(restarted) }()

	restartedListener, err := ListenQuic(restarted, serverTLS)
	if err != nil {
		t.Fatalf("listen after restart: %v", err)
	}
	defer func() { _ = restartedListener.Close() }()

	go func() {
		for {
			if _, aerr := restartedListener.Accept(context.Background()); aerr != nil {
				return
			}
		}
	}()

	start := time.Now()
	stream, err := clientConn.OpenStream()
	if err != nil {
		t.Fatalf("open stream against restarted server: %v", err)
	}
	if _, werr := stream.Write(make([]byte, 512)); werr != nil {
		t.Logf("stream write returned %v", werr)
	}

	select {
	case <-clientConn.Context().Done():
		elapsed := time.Since(start)
		t.Logf("agent noticed the restarted server after %v", elapsed)
		if elapsed >= quicMaxIdleTimeout {
			t.Fatalf("agent needed %v, no faster than the idle timeout; "+
				"the stateless reset never arrived", elapsed)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("agent never noticed the restarted server; it would stay " +
			"unusable until the idle timeout")
	}
}
