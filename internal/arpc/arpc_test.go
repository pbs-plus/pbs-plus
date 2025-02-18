package arpc

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// reverse is a helper function used in one of the tests.
func reverse(s string) string {
	chars := []rune(s)
	n := len(chars)
	for i := 0; i < n/2; i++ {
		chars[i], chars[n-1-i] = chars[n-1-i], chars[i]
	}
	return string(chars)
}

// TestARPCSession sets up a single in‑memory connection with a server and a client
// session. The server registers an "echo" handler while the client registers a "reverse"
// handler. It performs one RPC call in each direction.
func TestARPCSession(t *testing.T) {
	// Create an in‑memory connection.
	serverConn, clientConn := net.Pipe()

	// Create the server session.
	serverSession, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("NewServerSession error: %v", err)
	}
	// Create the client session.
	clientSession, err := NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("NewClientSession error: %v", err)
	}

	// Server router with an "echo" handler.
	serverRouter := NewRouter()
	serverRouter.Handle("echo", func(req Request) (Response, error) {
		var msg string
		if err := json.Unmarshal(req.Payload, &msg); err != nil {
			return Response{Status: 400, Message: "invalid payload"}, err
		}
		return Response{Status: 200, Data: msg}, nil
	})

	// Start serving incoming streams on the server side.
	serverServeErr := make(chan error, 1)
	go func() {
		serverServeErr <- serverSession.Serve(serverRouter)
	}()

	// Client router with a "reverse" handler.
	clientRouter := NewRouter()
	clientRouter.Handle("reverse", func(req Request) (Response, error) {
		var msg string
		if err := json.Unmarshal(req.Payload, &msg); err != nil {
			return Response{Status: 400, Message: "invalid payload"}, err
		}
		return Response{Status: 200, Data: reverse(msg)}, nil
	})

	// Start serving on the client side.
	clientServeErr := make(chan error, 1)
	go func() {
		clientServeErr <- clientSession.Serve(clientRouter)
	}()

	// Allow goroutines to start.
	time.Sleep(50 * time.Millisecond)

	// --- Client-to-Server RPC Call ---
	resp, err := clientSession.Call("echo", "hello")
	if err != nil {
		t.Fatalf("client Call echo error: %v", err)
	}
	if resp.Status != 200 {
		t.Errorf("client Call echo: expected status 200, got %d", resp.Status)
	}
	if dataStr, ok := resp.Data.(string); !ok || dataStr != "hello" {
		// Some JSON decoders may wrap strings or return a different type.
		dataStr = strings.TrimSpace(stringMustMarshal(resp.Data))
		if dataStr != "\"hello\"" && dataStr != "hello" {
			t.Errorf("client Call echo: expected data \"hello\", got %v", resp.Data)
		}
	}

	// --- Server-to-Client RPC Call ---
	resp2, err := serverSession.Call("reverse", "world")
	if err != nil {
		t.Fatalf("server Call reverse error: %v", err)
	}
	if resp2.Status != 200 {
		t.Errorf("server Call reverse: expected status 200, got %d", resp2.Status)
	}
	if dataStr, ok := resp2.Data.(string); !ok || dataStr != "dlrow" {
		dataStr = strings.TrimSpace(stringMustMarshal(resp2.Data))
		if dataStr != "\"dlrow\"" && dataStr != "dlrow" {
			t.Errorf("server Call reverse: expected data \"dlrow\", got %v", resp2.Data)
		}
	}

	// Clean up: close both sessions.
	if err := clientSession.Close(); err != nil {
		t.Errorf("clientSession.Close error: %v", err)
	}
	if err := serverSession.Close(); err != nil {
		t.Errorf("serverSession.Close error: %v", err)
	}

	// Allow Serve goroutines to exit.
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-serverServeErr:
		if err == nil {
			t.Log("server Serve exited cleanly")
		}
	default:
		t.Log("server Serve goroutine did not return error immediately")
	}
	select {
	case err := <-clientServeErr:
		if err == nil {
			t.Log("client Serve exited cleanly")
		}
	default:
		t.Log("client Serve goroutine did not return error immediately")
	}
}

// TestInvalidMethod ensures that an RPC call to an unregistered method returns an error.
func TestInvalidMethod(t *testing.T) {
	serverConn, clientConn := net.Pipe()

	serverSession, err := NewServerSession(serverConn, nil)
	if err != nil {
		t.Fatalf("NewServerSession error: %v", err)
	}
	clientSession, err := NewClientSession(clientConn, nil)
	if err != nil {
		t.Fatalf("NewClientSession error: %v", err)
	}

	serverRouter := NewRouter()
	serverRouter.Handle("echo", func(req Request) (Response, error) {
		var msg string
		if err := json.Unmarshal(req.Payload, &msg); err != nil {
			return Response{Status: 400, Message: "invalid payload"}, err
		}
		return Response{Status: 200, Data: msg}, nil
	})

	go serverSession.Serve(serverRouter)

	resp, err := clientSession.Call("nonexistent", "test")
	if err != nil {
		t.Fatalf("Call to nonexistent method returned error: %v", err)
	}
	if resp.Status != 404 {
		t.Errorf("expected status 404 for nonexistent method, got %d", resp.Status)
	}
	if !strings.Contains(resp.Message, "method not found") {
		t.Errorf("expected error message about missing method, got %s", resp.Message)
	}

	if err := clientSession.Close(); err != nil {
		t.Errorf("clientSession.Close error: %v", err)
	}
	if err := serverSession.Close(); err != nil {
		t.Errorf("serverSession.Close error: %v", err)
	}
}

// TestConcurrentSessions creates 1000 concurrent two-way sessions.
// Each session creates a pair of in‑memory connections with its own client and server.
// The server registers an "add" method that sums two integers, and the client calls it.
// The test verifies that all calls return the expected sum.
func TestConcurrentSessions(t *testing.T) {
	const numSessions = 10000
	var wg sync.WaitGroup
	wg.Add(numSessions)

	errCh := make(chan error, numSessions)

	for i := 0; i < numSessions; i++ {
		go func(i int) {
			defer wg.Done()

			// Create a pair of in‑memory connections.
			serverConn, clientConn := net.Pipe()

			// Create sessions.
			sSession, err := NewServerSession(serverConn, nil)
			if err != nil {
				errCh <- fmt.Errorf("session %d: error creating server session: %v", i, err)
				return
			}
			cSession, err := NewClientSession(clientConn, nil)
			if err != nil {
				errCh <- fmt.Errorf("session %d: error creating client session: %v", i, err)
				return
			}

			// Server router with an "add" handler.
			serverRouter := NewRouter()
			serverRouter.Handle("add", func(req Request) (Response, error) {
				var params struct {
					A int `json:"A"`
					B int `json:"B"`
				}
				if err := json.Unmarshal(req.Payload, &params); err != nil {
					return Response{Status: 400, Message: "invalid payload"}, err
				}
				return Response{Status: 200, Data: params.A + params.B}, nil
			})

			// Start serving on the server side.
			go func() {
				// We ignore errors here as closing the session will result in an error.
				_ = sSession.Serve(serverRouter)
			}()

			// Brief pause to ensure the Serve goroutine starts.
			time.Sleep(1 * time.Millisecond)

			// Client makes a call to "add" with the payload {A: i, B: i}.
			payload := struct {
				A int `json:"A"`
				B int `json:"B"`
			}{A: i, B: i}

			resp, err := cSession.Call("add", payload)
			if err != nil {
				errCh <- fmt.Errorf("session %d: call error: %v", i, err)
				return
			}
			if resp.Status != 200 {
				errCh <- fmt.Errorf("session %d: unexpected status: %d", i, resp.Status)
				return
			}
			// JSON numbers are typically decoded as float64.
			var result int
			switch v := resp.Data.(type) {
			case float64:
				result = int(v)
			case int:
				result = v
			default:
				errCh <- fmt.Errorf("session %d: unexpected data type %T", i, resp.Data)
				return
			}
			if result != 2*i {
				errCh <- fmt.Errorf("session %d: expected %d, got %d", i, 2*i, result)
			}

			// Tidy up.
			if err := cSession.Close(); err != nil {
				errCh <- fmt.Errorf("session %d: client session close error: %v", i, err)
			}
			if err := sSession.Close(); err != nil {
				errCh <- fmt.Errorf("session %d: server session close error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// stringMustMarshal is a helper that marshals a value to JSON.
// It is used for debugging purposes.
func stringMustMarshal(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}
