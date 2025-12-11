package arpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/xtaci/smux"
)

type latencyConn struct {
	net.Conn
	delay time.Duration
}

func (l *latencyConn) randomDelay() {
	jitter := time.Duration(rand.Int63n(int64(l.delay)))
	time.Sleep(l.delay + jitter)
}

func (l *latencyConn) Read(b []byte) (n int, err error) {
	l.randomDelay()
	return l.Conn.Read(b)
}

func (l *latencyConn) Write(b []byte) (n int, err error) {
	l.randomDelay()
	return l.Conn.Write(b)
}

func setupSessionWithRouter(t *testing.T, router Router) (clientSession *Session, cleanup func()) {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	const simulatedLatency = 5 * time.Millisecond
	serverConn = &latencyConn{Conn: serverConn, delay: simulatedLatency}
	clientConn = &latencyConn{Conn: clientConn, delay: simulatedLatency}

	serverSession, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create server session: %v", err)
	}

	clientSession, err = NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	serverSession.SetRouter(router)

	done := make(chan struct{})

	go func() {
		_ = serverSession.Serve()
		close(done)
	}()

	cleanup = func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
	}

	return clientSession, cleanup
}

func TestRouterServeStream_Echo(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	serverSession, err := smux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create smux server session: %v", err)
	}
	clientSession, err := smux.Client(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create smux client session: %v", err)
	}

	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		return Response{
			Status: 200,
			Data:   req.Payload,
		}, nil
	})

	var (
		wg     sync.WaitGroup
		srvErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, err := serverSession.AcceptStream()
		if err != nil {
			srvErr = err
			return
		}
		router.ServeStream(stream)
	}()

	clientStream, err := clientSession.OpenStream()
	if err != nil {
		t.Fatalf("failed to open client stream: %v", err)
	}

	payload := StringMsg("hello")
	payloadBytes, err := payload.Encode()
	if err != nil {
		t.Fatalf("failed to encode payload: %v", err)
	}

	req := Request{
		Method:  "echo",
		Payload: payloadBytes,
	}

	reqBytes, err := req.Encode()
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	if _, err := clientStream.Write(reqBytes); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	respBuf := make([]byte, 1024)
	n, err := clientStream.Read(respBuf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var resp Response
	if err := resp.Decode(respBuf[:n]); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	var echoed StringMsg
	if err := echoed.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("expected data 'hello', got %q", echoed)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for ServeStream to finish")
	}

	if srvErr != nil {
		t.Fatalf("server error during AcceptStream: %v", srvErr)
	}
}

func TestSessionCall_Success(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	resp, err := clientSession.Call("ping", nil)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	var pong StringMsg
	if err := pong.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode pong: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected pong response, got %q", pong)
	}
}

func TestSessionCall_Concurrency(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	const numClients = 100
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := MapStringIntMsg{"client": id}
			resp, err := clientSession.Call("ping", &payload)
			if err != nil {
				t.Errorf("Client %d error: %v", id, err)
				return
			}
			if resp.Status != 200 {
				t.Errorf("Client %d: expected status 200, got %d", id, resp.Status)
			}
			var pong StringMsg
			if err := pong.Decode(resp.Data); err != nil {
				t.Errorf("Client %d: failed to decode: %v", id, err)
				return
			}
			if pong != "pong" {
				t.Errorf("Client %d: expected 'pong', got %q", id, pong)
			}
		}(i)
	}

	wg.Wait()
}

func TestCallContext_Timeout(t *testing.T) {
	router := NewRouter()
	router.Handle("slow", func(req Request) (Response, error) {
		time.Sleep(200 * time.Millisecond)
		var done StringMsg = "done"
		doneBytes, _ := done.Encode()
		return Response{
			Status: 200,
			Data:   doneBytes,
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := clientSession.CallMsg(ctx, "slow", nil)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestCallBinary_Success(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	serverSess, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create server session: %v", err)
	}
	clientSess, err := NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	go func() {
		curSession := serverSess.muxSess.Load()
		stream, err := curSession.AcceptStream()
		if err != nil {
			t.Errorf("server: AcceptStream error: %v", err)
			return
		}
		defer stream.Close()

		reqBuf := make([]byte, 1024)
		n, err := stream.Read(reqBuf)
		if err != nil {
			t.Errorf("server: error reading request: %v", err)
			return
		}

		var req Request
		if err := req.Decode(reqBuf[:n]); err != nil {
			t.Errorf("server: error decoding request: %v", err)
			return
		}

		binaryData := []byte("hello world")

		resp := Response{Status: 213}
		respBytes, err := resp.Encode()
		if err != nil {
			t.Errorf("server: error encoding response: %v", err)
			return
		}

		if _, err := stream.Write(respBytes); err != nil {
			t.Errorf("server: error writing response: %v", err)
			return
		}

		r := bytes.NewReader(binaryData)
		if err := binarystream.SendDataFromReader(r, len(binaryData), stream); err != nil {
			t.Errorf("server: error writing response: %v", err)
			return
		}
	}()

	buffer := make([]byte, 1024)
	n, err := clientSess.CallBinary(context.Background(), "buffer", nil, buffer)
	if err != nil {
		t.Fatalf("client: CallBinary error: %v", err)
	}

	expected := "hello world"
	if n != len(expected) {
		t.Fatalf("expected %d bytes, got %d", len(expected), n)
	}
	got := string(buffer[:n])
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func TestCallMsg_ErrorResponse(t *testing.T) {
	router := NewRouter()
	router.Handle("error", func(req Request) (Response, error) {
		return Response{}, errors.New("test error")
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	data, err := clientSession.CallMsg(context.Background(), "error", nil)
	if err == nil {
		t.Fatal("expected error response from CallMsg, got nil")
	}
	if !strings.Contains(err.Error(), "test error") {
		t.Fatalf("expected error to contain 'test error', got: %v", err)
	}
	if data != nil {
		t.Fatalf("expected no returned data on error response, got: %v", data)
	}
}

func TestCallBinary_ErrorResponse(t *testing.T) {
	router := NewRouter()
	router.Handle("buffer_error", func(req Request) (Response, error) {
		return Response{}, errors.New("buffer error occurred")
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	buffer := make([]byte, 1024)
	n, err := clientSession.CallBinary(context.Background(), "buffer_error", nil, buffer)
	if err == nil {
		t.Fatal("expected error response from CallBinary, got nil")
	}
	if !strings.Contains(err.Error(), "buffer error occurred") {
		t.Fatalf("expected error message to contain 'buffer error occurred', got: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes read on error response, got %d", n)
	}
}

func TestCallBinary_Concurrency(t *testing.T) {
	router := NewRouter()
	router.Handle("binary_concurrent", func(req Request) (Response, error) {
		var payload MapStringIntMsg
		id := 0
		if req.Payload != nil {
			if err := payload.Decode(req.Payload); err == nil {
				if v, ok := payload["id"]; ok {
					id = v
				}
			}
		}

		dataStr := fmt.Sprintf("binary data for client %d", id)
		binaryData := []byte(dataStr)

		return Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				r := bytes.NewReader(binaryData)
				_ = binarystream.SendDataFromReader(r, len(binaryData), stream)
			},
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	const numClients = 50
	var clientWg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		clientWg.Add(1)
		go func(id int) {
			defer clientWg.Done()
			payload := MapStringIntMsg{"id": id}
			expected := fmt.Sprintf("binary data for client %d", id)
			buffer := make([]byte, len(expected)+100)
			n, err := clientSession.CallBinary(context.Background(), "binary_concurrent", &payload, buffer)
			if err != nil {
				t.Errorf("client %d: CallBinary error: %v", id, err)
				return
			}
			if n != len(expected) {
				t.Errorf("client %d: expected %d bytes, got %d", id, len(expected), n)
				return
			}
			if got := string(buffer[:n]); got != expected {
				t.Errorf("client %d: expected %q, got %q", id, expected, got)
			}
		}(i)
	}
	clientWg.Wait()
}

func setupSessionWithRouterForBenchmark(b *testing.B, router Router) (clientSession *Session, cleanup func()) {
	b.Helper()

	clientConn, serverConn := net.Pipe()

	serverSession, err := NewServerSession(serverConn, nil)
	if err != nil {
		b.Fatalf("failed to create server session: %v", err)
	}

	clientSession, err = NewClientSession(clientConn, nil)
	if err != nil {
		b.Fatalf("failed to create client session: %v", err)
	}

	serverSession.SetRouter(router)

	done := make(chan struct{})

	go func() {
		_ = serverSession.Serve()
		close(done)
	}()

	cleanup = func() {
		_ = clientSession.Close()
		_ = serverSession.Close()
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
	}

	return clientSession, cleanup
}

func BenchmarkSessionCall(b *testing.B) {
	const (
		numClients        = 100
		requestsPerClient = 100
	)

	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouterForBenchmark(b, router)
	defer cleanup()

	b.ReportAllocs()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numClients)

		for clientID := 0; clientID < numClients; clientID++ {
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < requestsPerClient; j++ {
					resp, err := clientSession.Call("ping", nil)
					if err != nil {
						b.Errorf("Client %d: Call failed: %v", clientID, err)
						return
					}
					if resp.Status != 200 {
						b.Errorf("Client %d: Expected status 200, got %d", clientID, resp.Status)
					}
					var pong StringMsg
					if err := pong.Decode(resp.Data); err != nil {
						b.Errorf("Client %d: Failed to decode response: %v", clientID, err)
					}
					if pong != "pong" {
						b.Errorf("Client %d: Expected 'pong', got %q", clientID, pong)
					}
				}
			}(clientID)
		}

		wg.Wait()
	}
	b.StopTimer()
}

type fakeTLSConn struct {
	net.Conn
	closed atomic.Bool
}

func (f *fakeTLSConn) Close() error {
	f.closed.Store(true)
	return f.Conn.Close()
}

type fakeTLSDialer struct {
	mu     sync.Mutex
	conns  []net.Conn
	index  int
	errors []error
}

func (d *fakeTLSDialer) next() (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.index < len(d.errors) && d.errors[d.index] != nil {
		err := d.errors[d.index]
		d.index++
		return nil, err
	}
	if d.index >= len(d.conns) {
		return nil, errors.New("no more conns")
	}
	c := d.conns[d.index]
	d.index++
	return c, nil
}

func TestMaintainTLSTunnel_ReconnectsOnDisconnect(t *testing.T) {
	server1, client1 := net.Pipe()
	server2, client2 := net.Pipe()

	serverSess1, err := NewServerSession(server1, nil)
	if err != nil {
		t.Fatalf("failed to create server session 1: %v", err)
	}
	serverSess2, err := NewServerSession(server2, nil)
	if err != nil {
		t.Fatalf("failed to create server session 2: %v", err)
	}

	cliSess, err := NewClientSession(client1, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{Status: 200, Data: pongBytes}, nil
	})
	serverSess1.SetRouter(router)
	serverSess2.SetRouter(router)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = serverSess1.Serve() }()
	go func() { _ = serverSess2.Serve() }()

	cliSess.state.Store(int32(StateConnected))

	dialer := &fakeTLSDialer{
		conns: []net.Conn{
			&fakeTLSConn{Conn: client1},
			&fakeTLSConn{Conn: client2},
		},
		errors: []error{nil, nil},
	}

	origDial := tlsDialFunc
	defer func() { tlsDialFunc = origDial }()
	tlsDialFunc = func(network, addr string, config *tls.Config) (net.Conn, error) {
		return dialer.next()
	}

	headers := make(map[string][]string)

	go maintainTLSTunnel(ctx, "unused:0", headers, &tls.Config{}, cliSess)

	time.Sleep(50 * time.Millisecond)

	_, _ = cliSess.Call("ping", nil)

	first := cliSess.muxSess.Load()
	if first == nil {
		t.Fatalf("expected initial session")
	}

	_ = first.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cur := cliSess.muxSess.Load()
		if cur != nil && !cur.IsClosed() && cur != first {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cur := cliSess.muxSess.Load()
	if cur == nil || cur.IsClosed() || cur == first {
		t.Fatalf("expected session to be re-established")
	}

	resp, err := cliSess.Call("ping", nil)
	if err != nil {
		t.Fatalf("call after reconnect failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected 200 after reconnect, got %d", resp.Status)
	}
}

var tlsDialFunc = func(network, addr string, config *tls.Config) (net.Conn, error) {
	return tls.Dial(network, addr, config)
}
