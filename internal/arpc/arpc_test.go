package arpc

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fastjson"
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

// TestCallJSONWithBuffer_Success verifies that CallJSONWithBuffer correctly
// reads the metadata and then the binary payload written by a custom server.
func TestCallJSONWithBuffer_Success(t *testing.T) {
	// Create an in-memory connection pair.
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Create server and client smux sessions wrapped in our Session type.
	serverSess, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("failed to create server session: %v", err)
	}
	clientSess, err := NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("failed to create client session: %v", err)
	}

	// Launch a goroutine to simulate a buffered-call handler on the server side.
	// Instead of routing via ServeStream (which always writes a JSON reply),
	// this goroutine will accept a stream, read the request (discarding its
	// contents), then write a metadata JSON (with bytes_available and eof)
	// followed (after a slight delay) by the binary payload.
	go func() {
		// Accept a stream from the underlying smux session.
		stream, err := serverSess.sess.AcceptStream()
		if err != nil {
			t.Errorf("server: AcceptStream error: %v", err)
			return
		}
		defer stream.Close()

		reader := bufio.NewReader(stream)
		writer := bufio.NewWriter(stream)

		// Read and discard the complete request (which contains the method,
		// payload, and direct-buffer header).
		_, err = reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("server: error reading request: %v", err)
			return
		}

		// Prepare the binary payload.
		binaryData := []byte("hello world")
		dataLen := len(binaryData)

		// Build metadata JSON using a pooled fastjson.Arena.
		arena := arenaPool.Get().(*fastjson.Arena)
		metaObj := arena.NewObject()
		metaObj.Set("status", arena.NewNumberInt(200))
		dataMeta := arena.NewObject()
		dataMeta.Set("bytes_available", arena.NewNumberInt(dataLen))
		// Use fastjson.MustParse to create a boolean value.
		dataMeta.Set("eof", fastjson.MustParse("true"))
		metaObj.Set("data", dataMeta)
		metaBytes := metaObj.MarshalTo(nil)
		metaBytes = append(metaBytes, '\n')
		arena.Reset()
		arenaPool.Put(arena)

		// Write the metadata first.
		if _, err := writer.Write(metaBytes); err != nil {
			t.Errorf("server: error writing metadata: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			t.Errorf("server: error flushing metadata: %v", err)
			return
		}

		// Sleep briefly to help ensure the client’s first read retrieves just
		// the metadata and not some of the binary payload.
		time.Sleep(50 * time.Millisecond)

		// Write the binary payload.
		if _, err := writer.Write(binaryData); err != nil {
			t.Errorf("server: error writing binary data: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			t.Errorf("server: error flushing binary data: %v", err)
			return
		}
	}()

	// On the client side, use CallJSONWithBuffer to send a request.
	// (The server ignores the request payload; it will respond using our custom
	// protocol above.)
	buffer := make([]byte, 64)
	n, eof, err := clientSess.CallJSONWithBuffer(context.Background(), "buffer", nil, buffer)
	if err != nil {
		t.Fatalf("client: CallJSONWithBuffer error: %v", err)
	}

	// Verify that we received the expected binary payload.
	expected := "hello world"
	if n != len(expected) {
		t.Fatalf("expected %d bytes, got %d", len(expected), n)
	}
	if got := string(buffer[:n]); got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
	if !eof {
		t.Fatal("expected eof to be true")
	}
}
