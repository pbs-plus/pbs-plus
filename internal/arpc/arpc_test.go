package arpc

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtaci/smux"
)

// ---------------------------------------------------------------------
// Helper: setupSessionWithRouter
//
// Creates a new pair of connected sessions (client/server) using net.Pipe.
// The server session immediately begins serving on the provided router.
// The function returns the client-side session (which is used for calls)
// plus a cleanup function to shut down both sessions.
// ---------------------------------------------------------------------
func setupSessionWithRouter(t *testing.T, router *Router) (clientSession *Session, cleanup func()) {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	serverSession, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create server session: %v", err)
	}

	clientSession, err = NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	done := make(chan struct{})

	// Start the server-session in a goroutine. Serve() will continuously
	// accept streams until the session is closed.
	go func() {
		// Note: any error returned from Serve is ignored.
		_ = serverSession.Serve(router)
		close(done)
	}()

	cleanup = func() {
		_ = clientSession.Close()
		_ = serverSession.Close()

		// Wait a little for the Serve goroutine to finish.
		select {
		case <-done:
		case <-time.After(100 * time.Millisecond):
		}
	}

	return clientSession, cleanup
}

// ---------------------------------------------------------------------
// Test 1: Router.ServeStream working as expected (Echo handler).
// We simulate a single JSON request/response using a net.Pipe as the
// underlying stream.
// ---------------------------------------------------------------------
func TestRouterServeStream_Echo(t *testing.T) {
	// Create an in-memory connection pair.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create a smux session on the server side.
	serverSession, err := smux.Server(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create smux server session: %v", err)
	}

	// Similarly, create a smux session on the client side.
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

	// On the server side, accept a stream from the session.
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
		// Pass the stream (of type *smux.Stream) to ServeStream.
		router.ServeStream(stream)
	}()

	// On the client side, open a stream.
	clientStream, err := clientSession.OpenStream()
	if err != nil {
		t.Fatalf("failed to open client stream: %v", err)
	}

	// Send a request over the client stream.
	req := Request{
		Method:  "echo",
		Payload: json.RawMessage(`"hello"`),
	}
	encoder := json.NewEncoder(clientStream)
	if err := encoder.Encode(req); err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	// Read the JSON response.
	decoder := json.NewDecoder(clientStream)
	var resp Response
	if err := decoder.Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	// Verify that the echoed data matches.
	var data string
	if err := json.Unmarshal([]byte(resp.Data.(string)), &data); err != nil {
		// If the returned Data is already a string, use it directly.
		data = resp.Data.(string)
	}
	if data != "hello" {
		t.Fatalf("expected data 'hello', got %v", data)
	}

	// Allow the server goroutine to finish, but don't block indefinitely.
	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(100 * time.Millisecond):
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
		return Response{
			Status: 200,
			Data:   "pong",
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
	pong, ok := resp.Data.(string)
	if !ok || pong != "pong" {
		t.Fatalf("expected pong response, got %#v", resp.Data)
	}
}

// ---------------------------------------------------------------------
// Test 3: Concurrency test.
// Spawn many concurrent goroutines making calls via the same session.
// ---------------------------------------------------------------------
func TestSessionCall_Concurrency(t *testing.T) {
	router := NewRouter()
	router.Handle("ping", func(req Request) (Response, error) {
		return Response{
			Status: 200,
			Data:   "pong",
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
			// Provide each caller with a small payload.
			payload := map[string]int{"client": id}
			resp, err := clientSession.Call("ping", payload)
			if err != nil {
				t.Errorf("Client %d error: %v", id, err)
				return
			}
			if resp.Status != 200 {
				t.Errorf("Client %d: expected status 200, got %d", id, resp.Status)
			}
			if pong, ok := resp.Data.(string); !ok || pong != "pong" {
				t.Errorf("Client %d: expected 'pong', got %v", id, resp.Data)
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
		// Simulate a long-running handler.
		time.Sleep(200 * time.Millisecond)
		return Response{
			Status: 200,
			Data:   "done",
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := clientSession.CallContext(ctx, "slow", nil)
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
		return Response{
			Status: 200,
			Data:   "pong",
		}, nil
	})

	// Start an initial client/server session.
	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	var dialCount int32

	// Define a custom dial function that creates a new net.Pipe pair
	// and immediately starts a new server session using the same router.
	dialFunc := func() (net.Conn, error) {
		atomic.AddInt32(&dialCount, 1)
		serverConn, clientConn := net.Pipe()
		go func() {
			sess, err := NewServerSession(serverConn, nil)
			if err != nil {
				t.Logf("server session error: %v", err)
				return
			}
			// Serve until the session is closed.
			_ = sess.Serve(router)
		}()
		return clientConn, nil
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return NewClientSession(conn, nil)
	}

	// Enable auto-reconnect on the client session.
	rc := &ReconnectConfig{
		AutoReconnect:  true,
		DialFunc:       dialFunc,
		UpgradeFunc:    upgradeFunc,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReconnectCtx:   context.Background(),
	}
	clientSession.EnableAutoReconnect(rc)

	// Simulate network failure by closing the underlying session.
	clientSession.mu.Lock()
	_ = clientSession.sess.Close()
	clientSession.mu.Unlock()

	// Now call "ping" which should trigger auto‑reconnect.
	resp, err := clientSession.Call("ping", nil)
	if err != nil {
		t.Fatalf("Call after disconnection failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}
	pong, ok := resp.Data.(string)
	if !ok || pong != "pong" {
		t.Fatalf("expected 'pong', got %v", resp.Data)
	}

	if atomic.LoadInt32(&dialCount) == 0 {
		t.Fatal("expected dial function to be called for reconnection")
	}
}

// ---------------------------------------------------------------------
// Test 6: CallWithHeaders.
// Verify that custom HTTP headers are delivered correctly within the call.
// ---------------------------------------------------------------------
func TestCallWithHeaders(t *testing.T) {
	router := NewRouter()
	router.Handle("user", func(req Request) (Response, error) {
		if req.Headers.Get("X-Test") != "value" {
			return Response{
				Status:  400,
				Message: "missing header",
			}, nil
		}
		return Response{
			Status: 200,
			Data:   "header ok",
		}, nil
	})

	clientSession, cleanup := setupSessionWithRouter(t, router)
	defer cleanup()

	ctx := context.Background()
	headers := http.Header{}
	headers.Set("X-Test", "value")
	resp, err := clientSession.CallWithHeaders(ctx, "user", nil, headers)
	if err != nil {
		t.Fatalf("CallWithHeaders failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}
	val, ok := resp.Data.(string)
	if !ok || val != "header ok" {
		t.Fatalf("expected 'header ok', got %v", resp.Data)
	}
}
