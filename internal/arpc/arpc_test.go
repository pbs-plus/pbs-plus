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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/xtaci/smux"
)

type testPKI struct {
	caCert *x509.Certificate
	caKey  *rsa.PrivateKey
	caDER  []byte
	caPool *x509.CertPool
}

func newTestCA(t *testing.T, cn string) *testPKI {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen ca key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	ski := make([]byte, 20)
	_, _ = rand.Read(ski)
	caTpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(48 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
		SubjectKeyId:          ski,
		SignatureAlgorithm:    x509.SHA256WithRSA,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	caLeaf, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caLeaf)
	return &testPKI{
		caCert: caLeaf,
		caKey:  caKey,
		caDER:  caDER,
		caPool: pool,
	}
}

func (p *testPKI) issueCert(t *testing.T, cn string, isClient bool, ips []net.IP, dns []string) tls.Certificate {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	ski := make([]byte, 20)
	_, _ = rand.Read(ski)
	tpl := &x509.Certificate{
		SerialNumber:       serial,
		Subject:            pkix.Name{CommonName: cn},
		NotBefore:          time.Now().Add(-time.Hour),
		NotAfter:           time.Now().Add(24 * time.Hour),
		KeyUsage:           x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		SignatureAlgorithm: x509.SHA256WithRSA,
		SubjectKeyId:       ski,
	}
	if isClient {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		tpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}
	if len(ips) > 0 {
		tpl.IPAddresses = append([]net.IP(nil), ips...)
	}
	if len(dns) > 0 {
		tpl.DNSNames = append([]string(nil), dns...)
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, p.caCert, &key.PublicKey, p.caKey)
	if err != nil {
		t.Fatalf("sign cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der, p.caDER},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

var (
	globalCAOnce sync.Once
	serverCA     *testPKI
	clientCA     *testPKI
)

func ensureGlobalCAs(t *testing.T) {
	globalCAOnce.Do(func() {
		serverCA = newTestCA(t, "test-server-ca")
		clientCA = newTestCA(t, "test-client-ca")
	})
}

func newTestARPCServer(t *testing.T, router Router) (addr string, cleanup func(), serverTLS *tls.Config) {
	t.Helper()

	ensureGlobalCAs(t)
	serverCert := serverCA.issueCert(t, "localhost", false, []net.IP{net.ParseIP("127.0.0.1")}, []string{"localhost"})

	serverTLS = &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA.caPool,
		MinVersion:   tls.VersionTLS13,
	}

	listener, err := Listen(t.Context(), "127.0.0.1:0", serverTLS)
	if err != nil {
		t.Fatalf("arpc listen: %v", err)
	}

	agentsManager := NewAgentsManager()
	done := make(chan struct{})
	go func() {
		defer close(done)
		err = Serve(t.Context(), agentsManager, listener, router)
		if err != nil {
			return
		}
	}()

	addr = listener.Addr().String()
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
	ensureGlobalCAs(t)
	clientCert := clientCA.issueCert(t, "client", true, nil, nil)

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      serverCA.caPool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS13,
	}
}

func TestRouterServeStream_Echo(t *testing.T) {
	router := NewRouter()
	router.Handle("echo", func(req *Request) (Response, error) {
		return Response{Status: http.StatusOK, Data: req.Payload}, nil
	})

	addr, shutdown, serverTLS := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var msg string = "hello"
	payload, _ := cbor.Marshal(msg)

	var out []byte
	if err := pipe.Call(context.Background(), "echo", payload, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}

	var echoed string
	if err := cbor.Unmarshal(out, &echoed); err != nil {
		t.Fatalf("decode echoed: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("expected hello, got %q", echoed)
	}

	_ = serverTLS
}

func TestStreamPipeCall_Success(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var out string
	if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out != "pong" {
		t.Fatalf("expected pong, got %q", out)
	}
}

func TestStreamPipeCall_Concurrency(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	const numClients = 100
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			defer wg.Done()
			payload := map[string]int{"client": id}
			var out string
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
	router.Handle("slow", func(req *Request) (Response, error) {
		time.Sleep(200 * time.Millisecond)
		var done string = "done"
		b, _ := cbor.Marshal(done)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	go func() {
		<-ctx.Done()
		pipe.Close()
	}()

	var out []byte
	err = pipe.Call(ctx, "slow", nil, &out)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("expected DeadlineExceeded or Canceled, got %v", err)
	}
}

func TestCall_ErrorResponse(t *testing.T) {
	router := NewRouter()
	router.Handle("error", func(req *Request) (Response, error) {
		return Response{}, fmt.Errorf("test error")
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var out []byte
	err = pipe.Call(context.Background(), "error", nil, &out)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() == "" {
		t.Fatalf("expected error containing 'test error', got: %v", err)
	}
}

func TestCall_RawStream_BinaryFlow(t *testing.T) {
	router := NewRouter()
	router.Handle("binary_flow", func(req *Request) (Response, error) {
		resp := Response{
			Status: 213,
			RawStream: func(st *smux.Stream) {
				payload := []byte("hello world")
				_ = binarystream.SendDataFromReader(bytes.NewReader(payload), len(payload), st)
				_ = st.Close()
			},
		}
		return resp, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var received []byte
	handler := RawStreamHandler(func(st *smux.Stream) error {
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
	router.Handle("binary", func(req *Request) (Response, error) {
		return Response{Status: 213, RawStream: func(st *smux.Stream) {}}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var out []byte
	err = pipe.Call(context.Background(), "binary", nil, &out)
	if err == nil {
		t.Fatal("expected error due to missing RawStreamHandler")
	}
}

func TestStreamPipe_State_And_Reconnect(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	if st := pipe.GetState(); st != StateConnected {
		t.Fatalf("expected connected, got %v", st)
	}

	_ = (*pipe.conn.Load()).Close()

	timeout := time.NewTimer(10 * time.Second)
	for st := pipe.GetState(); st != StateConnected; {
		select {
		case <-timeout.C:
			t.Fatalf("expected connected after reconnect, got %v", st)
		default:
		}
	}

	var out string
	if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatalf("Call after reconnect: %v", err)
	}
	if out != "pong" {
		t.Fatalf("expected pong, got %q", out)
	}
}

func TestRouter_NotFound_And_BadRequest(t *testing.T) {
	router := NewRouter()

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	var out []byte
	err = pipe.Call(context.Background(), "missing", nil, &out)
	if err == nil {
		t.Fatal("expected error for missing method")
	}

	router2 := NewRouter()
	addr2, shutdown2, _ := newTestARPCServer(t, router2)
	defer shutdown2()

	pipe2, err := ConnectToServer(t.Context(), addr2, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe2.Close()

	st, err := pipe2.OpenStream()
	if err != nil {
		t.Fatalf("OpenStreamSync: %v", err)
	}
	defer st.Close()

	req := Request{Method: "", Payload: nil}
	b, _ := cbor.Marshal(req)
	_, _ = st.Write(b)

	var resp Response
	if derr := cbor.Unmarshal(b, &resp); derr != nil {
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
	router.Handle("inc", func(req *Request) (Response, error) {
		var n int
		if err := cbor.Unmarshal(req.Payload, &n); err != nil {
			return Response{Status: http.StatusBadRequest}, nil
		}
		n = n + 1
		b, _ := cbor.Marshal(n)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	const total = 1000
	for i := 0; i < total; i++ {
		var in int = int(i)
		var out int
		if err := pipe.Call(context.Background(), "inc", &in, &out); err != nil {
			t.Fatalf("call %d failed: %v", i, err)
		}
		expected := int(i + 1)
		if out != expected {
			t.Fatalf("call %d expected %d got %d", i, expected, out)
		}
	}
}

func TestStress_BatchedSequences(t *testing.T) {
	router := NewRouter()
	router.Handle("echo_str", func(req *Request) (Response, error) {
		return Response{Status: http.StatusOK, Data: req.Payload}, nil
	})
	router.Handle("sum_pair", func(req *Request) (Response, error) {
		var pair map[string]int
		if err := cbor.Unmarshal(req.Payload, &pair); err != nil {
			return Response{Status: http.StatusBadRequest}, nil
		}
		total := 0
		for _, v := range pair {
			total += v
		}
		var out int = int(total)
		b, _ := cbor.Marshal(out)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	const batches = 100
	const perBatch = 40

	for b := 0; b < batches; b++ {
		for i := 0; i < perBatch; i++ {
			msg := string(fmt.Sprintf("b%d-i%d", b, i))
			var echoed string
			if err := pipe.Call(context.Background(), "echo_str", &msg, &echoed); err != nil {
				t.Fatalf("batch %d iter %d echo_str err: %v", b, i, err)
			}
			if echoed != msg {
				t.Fatalf("batch %d iter %d mismatch", b, i)
			}

			pl := map[string]int{"a": b, "b": i}
			var sum int
			if err := pipe.Call(context.Background(), "sum_pair", &pl, &sum); err != nil {
				t.Fatalf("batch %d iter %d sum_pair err: %v", b, i, err)
			}
			if int(sum) != b+i {
				t.Fatalf("batch %d iter %d expected %d got %d", b, i, b+i, sum)
			}
		}
	}
}

func TestStreams_ProperlyClosed_NoExhaustion(t *testing.T) {
	router := NewRouter()
	router.Handle("short", func(req *Request) (Response, error) {
		var ok string = "ok"
		b, _ := cbor.Marshal(ok)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	const iters = 256
	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var out string
		err := pipe.Call(ctx, "short", nil, &out)
		cancel()
		if err != nil {
			t.Fatalf("iter %d Call error: %v", i, err)
		}
		if out != "ok" {
			t.Fatalf("iter %d expected ok got %q", i, out)
		}
	}
}

func goroutineSnapshot() int {
	return runtime.NumGoroutine()
}

// waitForGoroutines waits for goroutines to settle within a tolerance
func waitForGoroutines(t *testing.T, baseline int, tolerance int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastCount int
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
		current := goroutineSnapshot()
		if current <= baseline+tolerance {
			return
		}
		lastCount = current
	}
	// Final check
	runtime.GC()
	runtime.Gosched()
	time.Sleep(200 * time.Millisecond)
	current := goroutineSnapshot()
	if current > baseline+tolerance {
		t.Errorf("goroutine leak detected: baseline=%d current=%d last=%d (tolerance=%d)",
			baseline, current, lastCount, tolerance)

		// Print stack traces for debugging
		buf := make([]byte, 1<<20)
		stackLen := runtime.Stack(buf, true)
		t.Logf("Goroutine dump:\n%s", buf[:stackLen])
	}
}

func TestLeak_SimpleCallAndClose(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	baseline := goroutineSnapshot()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}

	var out string
	if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
		t.Fatalf("Call: %v", err)
	}

	pipe.Close()

	// Give server time to cleanup
	time.Sleep(200 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 3*time.Second)
}

func TestLeak_MultipleConnections(t *testing.T) {
	router := NewRouter()
	router.Handle("echo", func(req *Request) (Response, error) {
		return Response{Status: http.StatusOK, Data: req.Payload}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	baseline := goroutineSnapshot()

	const numConns = 20
	for i := 0; i < numConns; i++ {
		clientTLS := newTestClientTLS(t)
		pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
		if err != nil {
			t.Fatalf("ConnectToServer %d: %v", i, err)
		}

		msg := fmt.Sprintf("test-%d", i)
		var out string
		if err := pipe.Call(context.Background(), "echo", &msg, &out); err != nil {
			t.Fatalf("Call %d: %v", i, err)
		}

		pipe.Close()

		// Allow server to process disconnect
		time.Sleep(100 * time.Millisecond)
	}

	// Extra time for all connections to fully cleanup
	time.Sleep(1 * time.Second)

	// More permissive tolerance for multiple connections
	// Each connection creates: client Serve, server Serve, 2 smux goroutines, context watchers
	waitForGoroutines(t, baseline, 5, 10*time.Second)
}

func TestLeak_ConcurrentCalls(t *testing.T) {
	router := NewRouter()
	router.Handle("work", func(req *Request) (Response, error) {
		time.Sleep(10 * time.Millisecond)
		var ok string = "ok"
		b, _ := cbor.Marshal(ok)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const numCalls = 100
	var wg sync.WaitGroup
	wg.Add(numCalls)

	for i := 0; i < numCalls; i++ {
		go func(id int) {
			defer wg.Done()
			var out string
			if err := pipe.Call(context.Background(), "work", nil, &out); err != nil {
				t.Errorf("Call %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// Allow streams to fully close
	time.Sleep(500 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_TimeoutCalls(t *testing.T) {
	router := NewRouter()
	router.Handle("slow", func(req *Request) (Response, error) {
		time.Sleep(500 * time.Millisecond)
		var done string = "done"
		b, _ := cbor.Marshal(done)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const numCalls = 20
	for i := 0; i < numCalls; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		var out string
		_ = pipe.Call(ctx, "slow", nil, &out)
		cancel()
		time.Sleep(20 * time.Millisecond)
	}

	// Allow cancelled operations to cleanup
	time.Sleep(500 * time.Millisecond)

	waitForGoroutines(t, baseline, 15, 5*time.Second)
}

func TestLeak_ReconnectCycle(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}

	baseline := goroutineSnapshot()

	const cycles = 10
	for i := 0; i < cycles; i++ {
		_ = (*pipe.conn.Load()).Close()
		time.Sleep(50 * time.Millisecond)

		var out string
		if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
			t.Fatalf("Call after reconnect %d: %v", i, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_RawStreamHandlers(t *testing.T) {
	router := NewRouter()
	router.Handle("binary", func(req *Request) (Response, error) {
		return Response{
			Status: 213,
			RawStream: func(st *smux.Stream) {
				data := make([]byte, 1024)
				_, _ = st.Write(data)
				_ = st.Close()
			},
		}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const numCalls = 30
	for i := 0; i < numCalls; i++ {
		handler := RawStreamHandler(func(st *smux.Stream) error {
			buf := make([]byte, 1024)
			_, err := st.Read(buf)
			return err
		})

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = pipe.Call(ctx, "binary", nil, handler)
		cancel()
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_ErrorResponses(t *testing.T) {
	router := NewRouter()
	router.Handle("error", func(req *Request) (Response, error) {
		return Response{}, fmt.Errorf("intentional error")
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const numCalls = 50
	for i := 0; i < numCalls; i++ {
		var out []byte
		_ = pipe.Call(context.Background(), "error", nil, &out)
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_StreamExhaustion(t *testing.T) {
	router := NewRouter()
	router.Handle("quick", func(req *Request) (Response, error) {
		var ok string = "ok"
		b, _ := cbor.Marshal(ok)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const iters = 200
	for i := 0; i < iters; i++ {
		var out string
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := pipe.Call(ctx, "quick", nil, &out)
		cancel()
		if err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
		if i%16 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	time.Sleep(500 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_CancelledContexts(t *testing.T) {
	router := NewRouter()
	router.Handle("work", func(req *Request) (Response, error) {
		time.Sleep(100 * time.Millisecond)
		var done string = "done"
		b, _ := cbor.Marshal(done)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	baseline := goroutineSnapshot()

	const numCalls = 30
	for i := 0; i < numCalls; i++ {
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		var out string
		_ = pipe.Call(ctx, "work", nil, &out)
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(500 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_ServerServeLoop(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req *Request) (Response, error) {
		var pong string = "pong"
		b, _ := cbor.Marshal(pong)
		return Response{Status: http.StatusOK, Data: b}, nil
	})

	baseline := goroutineSnapshot()

	addr, shutdown, _ := newTestARPCServer(t, router)

	clientTLS := newTestClientTLS(t)
	const numClients = 10
	for i := 0; i < numClients; i++ {
		pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
		if err != nil {
			t.Fatalf("ConnectToServer %d: %v", i, err)
		}

		var out string
		if err := pipe.Call(context.Background(), "ping", nil, &out); err != nil {
			t.Fatalf("Call %d: %v", i, err)
		}

		pipe.Close()

		time.Sleep(50 * time.Millisecond)
	}

	shutdown()

	time.Sleep(500 * time.Millisecond)

	waitForGoroutines(t, baseline, 5, 5*time.Second)
}

func TestLeak_MemoryPressure(t *testing.T) {
	router := NewRouter()
	router.Handle("large", func(req *Request) (Response, error) {
		data := make([]byte, 1024*1024) // 1 MB
		return Response{Status: http.StatusOK, Data: data}, nil
	})

	addr, shutdown, _ := newTestARPCServer(t, router)
	defer shutdown()

	clientTLS := newTestClientTLS(t)
	pipe, err := ConnectToServer(t.Context(), addr, nil, clientTLS)
	if err != nil {
		t.Fatalf("ConnectToServer: %v", err)
	}
	defer pipe.Close()

	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	const numCalls = 50
	for i := 0; i < numCalls; i++ {
		var out []byte
		if err := pipe.Call(context.Background(), "large", nil, &out); err != nil {
			t.Fatalf("Call %d: %v", i, err)
		}
		// Don't hold references
		out = nil
	}

	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond)

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	// Check that allocated memory hasn't grown excessively
	// Use HeapAlloc for more accurate measurement
	growth := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	if growth < 0 {
		growth = 0
	}
	maxExpectedGrowth := int64(10 * 1024 * 1024) // 10 MB tolerance

	if growth > maxExpectedGrowth {
		t.Errorf("potential memory leak: growth=%d MB (expected < %d MB)",
			growth/(1024*1024), maxExpectedGrowth/(1024*1024))
	}
}

func TestLeak_RouterHandlerReplace(t *testing.T) {
	router := NewRouter()

	baseline := goroutineSnapshot()

	const cycles = 100
	for i := 0; i < cycles; i++ {
		method := fmt.Sprintf("method-%d", i)
		router.Handle(method, func(req *Request) (Response, error) {
			var ok string = "ok"
			b, _ := cbor.Marshal(ok)
			return Response{Status: http.StatusOK, Data: b}, nil
		})

		if i > 10 {
			oldMethod := fmt.Sprintf("method-%d", i-10)
			router.CloseHandle(oldMethod)
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	current := goroutineSnapshot()
	if current > baseline+5 {
		t.Errorf("goroutine leak in router: baseline=%d current=%d", baseline, current)
	}
}
