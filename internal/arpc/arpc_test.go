package arpc

import (
	"bufio"
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/xtaci/smux"
)

// ---------------------------------------------------------------------
// Helper: setupSessionWithRouter
//
// Creates a new pair of connected sessions (client/server) using net.Pipe.
// The server session immediately begins serving on the provided router.
// The function returns the client‑side session (which is used for calls)
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

	// Start the server session in a goroutine. Serve() continuously accepts streams.
	go func() {
		_ = serverSession.Serve(router)
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
// We simulate a single JSON request/response using a net.Pipe as the
// underlying stream.
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

	// Build and send a request using the helper.
	reqBytes, err := buildRequestJSON("echo", "hello", nil)
	if err != nil {
		t.Fatalf("failed to build request JSON: %v", err)
	}
	if _, err := clientStream.Write(reqBytes); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read and parse the JSON response.
	respReader := bufio.NewReader(clientStream)
	line, err := respReader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		t.Fatalf("failed to read response: %v", err)
	}
	if len(line) == 0 {
		t.Fatalf("no response received")
	}

	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("failed to unmarshal response JSON: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	// Extract the echoed payload.
	var echoed string
	if err := json.Unmarshal(resp.Data, &echoed); err != nil {
		t.Fatalf("failed to unmarshal echoed data: %v", err)
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
			Data:   json.RawMessage(`"pong"`),
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
	var pong string
	if err := json.Unmarshal(resp.Data, &pong); err != nil {
		t.Fatalf("failed to unmarshal pong: %v", err)
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
		return Response{
			Status: 200,
			Data:   json.RawMessage(`"pong"`),
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
			payload := map[string]interface{}{"client": id}
			resp, err := clientSession.Call("ping", payload)
			if err != nil {
				t.Errorf("Client %d error: %v", id, err)
				return
			}
			if resp.Status != 200 {
				t.Errorf("Client %d: expected status 200, got %d", id, resp.Status)
			}
			var pong string
			if err := json.Unmarshal(resp.Data, &pong); err != nil {
				t.Errorf("Client %d: failed to unmarshal: %v", id, err)
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
		return Response{
			Status: 200,
			Data:   json.RawMessage(`"done"`),
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
			Data:   json.RawMessage(`"pong"`),
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
			_ = sess.Serve(router)
		}()
		return clientConn, nil
	}

	upgradeFunc := func(conn net.Conn) (*Session, error) {
		return NewClientSession(conn, nil)
	}

	// Enable auto‑reconnect on the client session.
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
	var pong string
	if err := json.Unmarshal(resp.Data, &pong); err != nil {
		t.Fatalf("failed to unmarshal pong: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected 'pong', got %q", pong)
	}

	if atomic.LoadInt32(&dialCount) == 0 {
		t.Fatal("expected dial function to be called for reconnection")
	}
}

// ---------------------------------------------------------------------
// Test 6: CallJSONWithBuffer_Success
//
// Verifies that CallJSONWithBuffer correctly reads the metadata and then the
// binary payload written by a custom server.
// ---------------------------------------------------------------------
func TestCallJSONWithBuffer_Success(t *testing.T) {
	// Create an in‑memory connection pair.
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

	// Launch a goroutine to simulate a buffered‑call handler on the server side.
	go func() {
		stream, err := serverSess.sess.AcceptStream()
		if err != nil {
			t.Errorf("server: AcceptStream error: %v", err)
			return
		}
		defer stream.Close()

		reader := bufio.NewReader(stream)
		writer := bufio.NewWriter(stream)

		// Read and discard the complete request.
		_, err = reader.ReadBytes('\n')
		if err != nil {
			t.Errorf("server: error reading request: %v", err)
			return
		}

		// Prepare the binary payload.
		binaryData := []byte("hello world")
		dataLen := len(binaryData)

		// Build metadata as a simple map.
		metaMap := map[string]interface{}{
			"status": 200,
			"data": map[string]interface{}{
				"bytes_available": dataLen,
				"eof":             true,
			},
		}
		metaBytes, err := json.Marshal(metaMap)
		if err != nil {
			t.Errorf("server: error marshaling metadata: %v", err)
			return
		}
		// Append newline as the delimiter.
		metaBytes = append(metaBytes, '\n')

		// Write the metadata.
		if _, err := writer.Write(metaBytes); err != nil {
			t.Errorf("server: error writing metadata: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			t.Errorf("server: error flushing metadata: %v", err)
			return
		}

		// Wait briefly to ensure the client reads only metadata first.
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
	buffer := make([]byte, 64)
	n, eof, err := clientSess.CallJSONWithBuffer(context.Background(), "buffer", nil, buffer)
	if err != nil {
		t.Fatalf("client: CallJSONWithBuffer error: %v", err)
	}

	expected := "hello world"
	if n != len(expected) {
		t.Fatalf("expected %d bytes, got %d", len(expected), n)
	}
	got := string(buffer[:n])
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
	if !eof {
		t.Fatal("expected eof to be true")
	}
}
