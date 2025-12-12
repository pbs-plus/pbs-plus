package arpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// --- Test helpers ---

func genSelfSignedCert(t *testing.T, cn string, isClient bool) tls.Certificate {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serialNumber, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: cn,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
	}
	if isClient {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		template.DNSNames = []string{cn}
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
	return cert
}

func newTestQUICServer(t *testing.T, router Router) (addr string, cleanup func(), serverTLS *tls.Config) {
	t.Helper()

	serverCert := genSelfSignedCert(t, "localhost", false)

	serverTLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		NextProtos:   []string{"h2", "http/1.1", "pbsarpc"},
	}

	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolve udp: %v", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}

	listener, err := quic.Listen(udpConn, serverTLS, &quic.Config{
		KeepAlivePeriod:    200 * time.Millisecond,
		MaxIncomingStreams: quicvarint.Max,
	})
	if err != nil {
		t.Fatalf("quic listen: %v", err)
	}

	agentsManager := NewAgentsManager()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept(context.Background())
			if err != nil {
				return
			}
			go func(c *quic.Conn) {
				if len(c.ConnectionState().TLS.PeerCertificates) == 0 {
					_ = c.CloseWithError(1, "client certificate required")
					return
				}

				pipe, id, err := agentsManager.GetOrCreateStreamPipe(c)
				if err != nil {
					return
				}
				defer func() { agentsManager.CloseStreamPipe(id) }()
				pipe.SetRouter(router)
				_ = pipe.Serve()
			}(conn)
		}
	}()

	addr = udpConn.LocalAddr().String()
	cleanup = func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(300 * time.Millisecond):
		}
	}
	return addr, cleanup, serverTLS
}

func newTestClientTLS(t *testing.T) *tls.Config {
	t.Helper()
	clientCert := genSelfSignedCert(t, "client", true)
	return &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		ServerName:         "localhost",
		NextProtos:         []string{"h2", "http/1.1", "pbsarpc"},
		InsecureSkipVerify: true,
	}
}

func dialTestPipe(t *testing.T, addr string, clientTLS *tls.Config) *StreamPipe {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := quic.DialAddr(ctx, addr, clientTLS, &quic.Config{
		KeepAlivePeriod:        200 * time.Millisecond,
		MaxStreamReceiveWindow: quicvarint.Max,
	})
	if err != nil {
		t.Fatalf("quic dial: %v", err)
	}
	pipe, err := NewStreamPipe(conn)
	if err != nil {
		t.Fatalf("new StreamPipe: %v", err)
	}
	return pipe
}

// --- Tests ---

func TestRouterServeStream_Echo(t *testing.T) {
	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		return Response{Status: http.StatusOK, Data: req.Payload}, nil
	})

	addr, shutdown, serverTLS := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(context.Background(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var msg StringMsg = "hello"
	payload, _ := msg.Encode()

	var out []byte
	if err := pipe.Call(context.Background(), "echo", payload, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}

	var echoed StringMsg
	if err := echoed.Decode(out); err != nil {
		t.Fatalf("decode echoed: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("expected hello, got %q", echoed)
	}

	_ = serverTLS
}

func TestStreamPipeCall_Success(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		b, _ := pong.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	var out StringMsg
	if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out != "pong" {
		t.Fatalf("expected pong, got %q", out)
	}
}

func TestStreamPipeCall_Concurrency(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		b, _ := pong.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	const numClients = 100
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			payload := MapStringIntMsg{"client": id}
			var out StringMsg
			if err := pipe.Call(context.Background(), "ping", &payload, &out); err != nil {
				t.Errorf("client %d: %v", id, err)
				return
			}
			if out != "pong" {
				t.Errorf("client %d: expected pong, got %q", id, out)
			}
		}(i)
	}
	wg.Wait()
}

func TestCallWithTimeout_DeadlineExceeded(t *testing.T) {
	router := NewRouter()
	router.Handle("slow", func(req Request) (Response, error) {
		time.Sleep(200 * time.Millisecond)
		var done StringMsg = "done"
		b, _ := done.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = pipe.Close()
	}()

	var out []byte
	err := pipe.Call(ctx, "slow", nil, &out)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected DeadlineExceeded or Canceled, got %v", err)
	}
}

func TestCall_ErrorResponse(t *testing.T) {
	router := NewRouter()
	router.Handle("error", func(req Request) (Response, error) {
		return Response{}, fmt.Errorf("test error")
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	var out []byte
	err := pipe.Call(context.Background(), "error", nil, &out)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() == "" {
		t.Fatalf("expected error containing 'test error', got: %v", err)
	}
}

func TestCall_RawStream_BinaryFlow(t *testing.T) {
	router := NewRouter()
	router.Handle("binary_flow", func(req Request) (Response, error) {
		resp := Response{
			Status: 213,
			RawStream: func(st *quic.Stream) {
				payload := []byte("hello world")
				_ = binarystream.SendDataFromReader(bytes.NewReader(payload), len(payload), st)
				_ = st.Close()
			},
		}
		return resp, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	var received []byte
	handler := RawStreamHandler(func(st *quic.Stream) error {
		buf := make([]byte, len("hello world"))
		n, err := binarystream.ReceiveDataInto(st, buf)
		if err != nil {
			return fmt.Errorf("receive failed: %w", err)
		}
		received = append(received[:0], buf[:n]...)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := pipe.Call(ctx, "binary_flow", nil, handler); err != nil {
		t.Fatalf("Call failed: %v", err)
	}

	if string(received) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(received))
	}
}

func TestCall_RawStream_HandlerMissing(t *testing.T) {
	router := NewRouter()
	router.Handle("binary", func(req Request) (Response, error) {
		return Response{Status: 213, RawStream: func(st *quic.Stream) {}}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	var out []byte
	err := pipe.Call(context.Background(), "binary", nil, &out)
	if err == nil {
		t.Fatal("expected error due to missing RawStreamHandler")
	}
}

func TestStreamPipe_State_And_Reconnect(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		b, _ := pong.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(context.Background(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	if st := pipe.GetState(); st != StateConnected {
		t.Fatalf("expected connected, got %v", st)
	}

	_ = pipe.Conn.CloseWithError(0, "test close")

	_ = pipe.Reconnect(context.Background())

	if st := pipe.GetState(); st != StateConnected {
		t.Fatalf("expected connected after reconnect, got %v", st)
	}

	var out StringMsg
	if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatalf("Call after reconnect: %v", err)
	}
	if out != "pong" {
		t.Fatalf("expected pong, got %q", out)
	}
}

func TestRouter_NotFound_And_BadRequest(t *testing.T) {
	router := NewRouter()

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	var out []byte
	err := pipe.Call(context.Background(), "missing", nil, &out)
	if err == nil {
		t.Fatal("expected error for missing method")
	}

	router2 := NewRouter()
	addr2, shutdown2, _ := newTestQUICServer(t, router2)
	defer shutdown2()

	pipe2 := dialTestPipe(t, addr2, clientTLS)
	defer pipe2.Close()

	st, err := pipe2.OpenStreamSync(context.Background())
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	defer st.Close()

	req := Request{Method: "", Payload: nil}
	b, _ := req.Encode()
	_, _ = st.Write(b)

	var resp Response
	dec := cbor.NewDecoder(st)
	if derr := dec.Decode(&resp); derr != nil {
		return
	}
	if resp.Status == http.StatusOK {
		t.Fatalf("expected non-200 for bad request, got 200")
	}
}

func TestSerializableError_Wrap_Unwrap(t *testing.T) {
	orig := &os.PathError{Op: "open", Path: "/nope", Err: os.ErrNotExist}
	se := WrapError(orig)
	if se == nil {
		t.Fatal("WrapError returned nil")
	}
	if se.ErrorType == "" || se.Message == "" {
		t.Fatal("missing fields in SerializableError")
	}

	err := UnwrapError(*se)
	var pe *os.PathError
	if !errors.As(err, &pe) {
		t.Fatalf("expected PathError, got %T", err)
	}
	if !errors.Is(pe.Err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", pe.Err)
	}
}

func TestStress_ConsecutiveCalls(t *testing.T) {
	router := NewRouter()
	router.Handle("inc", func(req Request) (Response, error) {
		var n IntMsg
		if err := n.Decode(req.Payload); err != nil {
			return Response{Status: http.StatusBadRequest}, nil
		}
		n = n + 1
		b, _ := n.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	const total = 100
	for i := 0; i < total; i++ {
		var in IntMsg = IntMsg(i)
		var out IntMsg
		if err := pipe.Call(context.Background(), "inc", &in, &out); err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		expected := IntMsg(i + 1)
		if out != expected {
			t.Fatalf("call %d expected %d got %d", i, expected, out)
		}
	}
}

func TestStress_BatchedSequences(t *testing.T) {
	router := NewRouter()
	router.Handle("echo_str", func(req Request) (Response, error) {
		return Response{Status: http.StatusOK, Data: req.Payload}, nil
	})
	router.Handle("sum_pair", func(req Request) (Response, error) {
		var pair MapStringIntMsg
		if err := pair.Decode(req.Payload); err != nil {
			return Response{Status: http.StatusBadRequest}, nil
		}
		total := 0
		for _, v := range pair {
			total += v
		}
		var out IntMsg = IntMsg(total)
		b, _ := out.Encode()
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestQUICServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe := dialTestPipe(t, addr, clientTLS)
	defer pipe.Close()

	const batches = 10
	const perBatch = 40

	for b := 0; b < batches; b++ {
		for i := 0; i < perBatch; i++ {
			msg := StringMsg(fmt.Sprintf("b%d-i%d", b, i))
			var echoed StringMsg
			if err := pipe.Call(context.Background(), "echo_str", &msg, &echoed); err != nil {
				t.Fatalf("batch %d iter %d echo_str err: %v", b, i, err)
			}
			if echoed != msg {
				t.Fatalf("batch %d iter %d mismatch", b, i)
			}

			pl := MapStringIntMsg{"a": b, "b": i}
			var sum IntMsg
			if err := pipe.Call(context.Background(), "sum_pair", &pl, &sum); err != nil {
				t.Fatalf("batch %d iter %d sum_pair err: %v", b, i, err)
			}
			if int(sum) != b+i {
				t.Fatalf("batch %d iter %d expected %d got %d", b, i, b+i, sum)
			}
		}
	}
}

// Simple contains helper to avoid pulling strings pkg repeatedly.
func contains(s, sub string) bool { return len(s) >= len(sub) && (stringIndex(s, sub) >= 0) }

// Minimal index to avoid extra imports; fine for tests.
func stringIndex(s, sub string) int {
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := range sub {
			if s[i+j] != sub[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
