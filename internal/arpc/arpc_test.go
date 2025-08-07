package arpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio"
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

// ---------------------------------------------------------------------
// Helper: setupSessionWithRouter
//
// Creates a new pair of connected sessions (client/server) using net.Pipe.
// The server session immediately begins serving on the provided router.
// The function returns the client‑side session (which is used for calls)
// plus a cleanup function to shut down both sessions.
// ---------------------------------------------------------------------
func setupSessionWithRouter(t *testing.T, router Router) (clientSession *Session, cleanup func()) {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	// Emulate network latency by wrapping the connections.
	// For example, here we simulate a constant 100ms latency.
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

	// Start the server session in a goroutine. Serve() continuously accepts streams.
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

// ---------------------------------------------------------------------
// Test 1: Router.ServeStream working as expected (Echo handler).
// We simulate a single request/response using a net.Pipe as the underlying stream.
// ---------------------------------------------------------------------
func TestRouterServeStream_Echo(t *testing.T) {
	// Create an in‑memory connection pair.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create smux sessions for server and client.
	serverSession, err := smux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create smux server session: %v", err)
	}
	clientSession, err := smux.Client(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create smux client session: %v", err)
	}

	// Create the router and register an "echo" handler.
	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		// Echo back the payload.
		return Response{
			Status: 200,
			Data:   req.Payload,
		}, nil
	})

	// On the server side, accept a stream.
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

	// On the client side, open a stream.
	clientStream, err := clientSession.OpenStream()
	if err != nil {
		t.Fatalf("failed to open client stream: %v", err)
	}

	// Build and send a request.
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

	// Read and decode the response.
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

	// Extract the echoed payload.
	var echoed StringMsg
	if err := echoed.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("expected data 'hello', got %q", echoed)
	}

	// Wait for the server goroutine to finish.
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

// ---------------------------------------------------------------------
// Test 2: Session.Call (simple call).
// Create connected client/server sessions and call a simple "ping" method.
// ---------------------------------------------------------------------
func TestSessionCall_Success(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		// Echo back "pong".
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

// ---------------------------------------------------------------------
// Test 3: Concurrency test.
// Spawn many concurrent goroutines making calls via the same session.
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// Test 4: CallContext with timeout.
// The server is deliberately slow. The client should abort the call.
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// Test 5: Auto-reconnect.
// Simulate a broken connection by closing the underlying session, and
// verify that a subsequent call automatically triggers reconnection.
// ---------------------------------------------------------------------
func TestAutoReconnect(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	// Start an initial client/server session.
	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	var dialCount int32

	// Create a custom dial function that creates a new net.Pipe pair and
	// immediately starts a new server session using the same router.
	dialFunc := func() (net.Conn, error) {
		atomic.AddInt32(&dialCount, 1)
		serverConn, clientConn := net.Pipe()
		go func() {
			sess, err := NewServerSession(serverConn, nil)
			if err != nil {
				t.Logf("server session error: %v", err)
				return
			}
			sess.SetRouter(router)
			_ = sess.Serve()
		}()
		return clientConn, nil
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return NewClientSession(conn, nil)
	}

	// Enable auto‑reconnect on the client session.
	rc := ReconnectConfig{
		AutoReconnect:  true,
		DialFunc:       dialFunc,
		UpgradeFunc:    upgradeFunc,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReconnectCtx:   context.Background(),
	}
	clientSession.EnableAutoReconnect(rc)

	// Simulate network failure by closing the underlying session.
	curMux := clientSession.muxSess.Load()
	curMux.Close()

	// Now call "ping" which should trigger auto‑reconnect.
	resp, err := clientSession.Call("ping", nil)
	if err != nil {
		t.Fatalf("Call after disconnection failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}
	var pong StringMsg
	if err := pong.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode pong: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected 'pong', got %q", pong)
	}

	if atomic.LoadInt32(&dialCount) == 0 {
		t.Fatal("expected dial function to be called for reconnection")
	}
}

// ---------------------------------------------------------------------
// Test 6: CallBinary_Success
//
// Verifies that CallBinary correctly reads the metadata and then the
// binary payload written by a custom server.
// ---------------------------------------------------------------------
func TestCallBinary_Success(t *testing.T) {
	// Create an in-memory connection pair.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create server and client sessions.
	serverSess, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create server session: %v", err)
	}
	clientSess, err := NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	// Launch a goroutine to simulate a buffered-call handler on the server side.
	go func() {
		curSession := serverSess.muxSess.Load()
		stream, err := curSession.AcceptStream()
		if err != nil {
			t.Errorf("server: AcceptStream error: %v", err)
			return
		}
		defer stream.Close()

		// Read and decode the request.
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

		// Prepare the binary payload
		binaryData := []byte("hello world")

		// Send the response
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

	// On the client side, use CallBinary to send a request.
	buffer, n, err := clientSess.CallBinary(context.Background(), "buffer", nil)
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

// ---------------------------------------------------------------------
// TestCallMsg_ErrorResponse verifies that when the server handler returns
// an error, the client can correctly parse the error response from CallMsg.
// ---------------------------------------------------------------------
func TestCallMsg_ErrorResponse(t *testing.T) {
	router := NewRouter()
	// Register a handler that deliberately returns an error.
	router.Handle("error", func(req Request) (Response, error) {
		// Returning an error here will trigger writeErrorResponse,
		// which wraps the error inside a Response with non‑200 status.
		return Response{}, errors.New("test error")
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	// CallMsg should return an error since the handler produced one.
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

// ---------------------------------------------------------------------
// TestCallBinary_ErrorResponse verifies that when the server handler
// returns an error during a buffered call, the client returns the expected error.
// ---------------------------------------------------------------------
func TestCallBinary_ErrorResponse(t *testing.T) {
	router := NewRouter()
	// Register a handler that simulates an error response.
	router.Handle("buffer_error", func(req Request) (Response, error) {
		// Trigger an error response; writeErrorResponse is used internally.
		return Response{}, errors.New("buffer error occurred")
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	// Prepare a buffer for the expected binary payload.
	_, n, err := clientSession.CallBinary(context.Background(), "buffer_error", nil)
	if err == nil {
		t.Fatal("expected error response from CallBinary, got nil")
	}
	if !strings.Contains(err.Error(), "buffer error occurred") {
		t.Fatalf("expected error message to contain 'buffer error occurred', got: %v", err)
	}
	// When an error response is returned, no binary data is expected.
	if n != 0 {
		t.Fatalf("expected 0 bytes read on error response, got %d", n)
	}
}

// TestCallBinary_Concurrency verifies that CallBinary works concurrently
// with different sets of binary data. For each client call, a unique payload
// (with an "id") is sent and the server replies with a binary message that
// includes the id.
func TestCallBinary_Concurrency(t *testing.T) {
	// Create a router and register a handler for "binary_concurrent".
	router := NewRouter()
	router.Handle("binary_concurrent", func(req Request) (Response, error) {
		// Decode the payload to extract the client ID.
		var payload MapStringIntMsg
		id := 0
		if req.Payload != nil {
			if err := payload.Decode(req.Payload); err == nil {
				if v, ok := payload["id"]; ok {
					id = v
				}
			}
		}

		// Prepare binary data that is unique for this client.
		dataStr := fmt.Sprintf("binary data for client %d", id)
		binaryData := []byte(dataStr)

		// Return a response with status 213 and a streaming callback.
		return Response{
			Status: 213,
			RawStream: func(stream *smux.Stream) {
				// Send the binary data to the client.
				r := bytes.NewReader(binaryData)
				if err := binarystream.SendDataFromReader(r, len(binaryData), stream); err != nil {
					t.Logf("server: error sending binary data for client %d: %v", id, err)
				}
			},
		}, nil
	})

	// Set up the client and server sessions with the router.
	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	// Spawn multiple concurrent client calls.
	const numClients = 50
	var clientWg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		clientWg.Add(1)
		go func(id int) {
			defer clientWg.Done()

			// Prepare a payload with a unique client ID.
			payload := MapStringIntMsg{"id": id}

			// Allocate a buffer to hold the binary response.
			buffer, n, err := clientSession.CallBinary(context.Background(), "binary_concurrent", &payload)
			if err != nil {
				t.Errorf("client %d: CallBinary error: %v", id, err)
				return
			}

			// Verify the response.
			expected := fmt.Sprintf("binary data for client %d", id)
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

	// Start the server session in a goroutine. Serve() continuously accepts streams.
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

// BenchmarkSessionCall benchmarks the performance of the Session.Call method
// with a high number of concurrent requests.
func BenchmarkSessionCall(b *testing.B) {
	// Define the number of concurrent clients and requests per client.
	const (
		numClients        = 100 // Number of concurrent clients
		requestsPerClient = 100 // Number of requests per client
	)

	// Set up the router with a simple "ping" handler.
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		// Simulate minimal processing time.
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	// Set up the client session and cleanup function.
	clientSession, cleanup := setupSessionWithRouterForBenchmark(b, router)
	defer cleanup()

	// Report memory allocations.
	b.ReportAllocs()

	// Run the benchmark.
	b.ResetTimer() // Reset the timer to exclude setup time.
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numClients)

		// Launch multiple clients in parallel.
		for clientID := 0; clientID < numClients; clientID++ {
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < requestsPerClient; j++ {
					// Make a "ping" call.
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

		// Wait for all clients to finish.
		wg.Wait()
	}
	b.StopTimer() // Stop the timer after the benchmark is complete.
}

func TestConnectToServer_NBIO(t *testing.T) {
	eng := nbio.NewEngine(nbio.Config{
		Network: "tcp",
		Addrs:   []string{":0"},
	})
	if err := eng.Start(); err != nil {
		t.Fatalf("failed to start engine: %v", err)
	}
	defer eng.Stop()

	var (
		serverSession *Session
		acceptErr     error
		readyCh       = make(chan struct{})
	)
	eng.OnOpen(func(c *nbio.Conn) {
		go func() {
			defer close(readyCh)
			// Read HTTP upgrade request
			buf := make([]byte, 4096)
			n, err := c.Read(buf)
			if err != nil {
				acceptErr = fmt.Errorf("server read: %w", err)
				c.Close()
				return
			}
			req := string(buf[:n])
			if !strings.Contains(req, "Upgrade: tcp") {
				acceptErr = fmt.Errorf("bad upgrade request: %q", req)
				c.Close()
				return
			}
			// Write HTTP 101 response
			_, err = c.Write([]byte("HTTP/1.1 101 Switching Protocols\r\n\r\n"))
			if err != nil {
				acceptErr = fmt.Errorf("server write: %w", err)
				c.Close()
				return
			}
			// Now run smux.Server on this connection
			sess, err := NewServerSession(c, nil)
			if err != nil {
				acceptErr = fmt.Errorf("smux server: %w", err)
				c.Close()
				return
			}
			serverSession = sess
			// Install a router
			router := NewRouter()
			router.Handle("ping", func(req Request) (Response, error) {
				return Response{Status: 200}, nil
			})
			sess.SetRouter(router)
			// Serve streams
			_ = sess.Serve()
		}()
	})

	addr := eng.Addrs[0]

	sess, err := ConnectToServer(
		context.Background(),
		false,
		addr,
		http.Header{},
		nil, // no TLS
	)
	if err != nil {
		t.Fatalf("ConnectToServer failed: %v", err)
	}
	defer func() {
		if sess != nil {
			sess.Close()
		}
		<-readyCh // wait for server goroutine to finish
		if serverSession != nil {
			serverSession.Close()
		}
		if acceptErr != nil {
			t.Fatalf("server error: %v", acceptErr)
		}
	}()

	// 4. Call "ping" and expect a 200 response
	resp, err := sess.CallWithTimeout(2*time.Second, "ping", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("unexpected status: %d", resp.Status)
	}
}
