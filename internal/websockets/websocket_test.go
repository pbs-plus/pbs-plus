//go:build linux

package websockets

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Add these test-specific variables
var (
	testServerURL   string
	testHeaders     = http.Header{"X-Test-Header": []string{"test-value"}}
	testMessageType = "test-message"
)

func TestWSClientBasicOperation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup test server
	ts := httptest.NewServer(http.HandlerFunc(echoHandler))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL:  wsURL,
		ClientID:   "test-client",
		Headers:    testHeaders,
		MaxRetries: 3,
	}, nil)

	var received atomic.Int32
	client.RegisterHandler(testMessageType, func(msg *Message) {
		received.Add(1)
	})

	client.Start()
	defer client.Close()

	// Test basic send/receive
	err := client.Send(Message{Type: testMessageType})
	require.NoError(t, err, "should send message without error")

	assert.Eventually(t, func() bool {
		return received.Load() == 1
	}, 2*time.Second, 100*time.Millisecond, "should receive message")

	// Test connection status
	assert.Equal(t, StateConnected, client.GetConnectionStatus(), "should show connected state")
}

func TestWSClientReconnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Setup flaky test server
	var connectionCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connectionCount.Load() < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		echoHandler(w, r)
	}))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL:  wsURL,
		ClientID:   "reconnect-client",
		Headers:    testHeaders,
		MaxRetries: 5,
	}, nil)

	var received atomic.Int32
	client.RegisterHandler(testMessageType, func(msg *Message) {
		received.Add(1)
	})

	client.Start()
	defer client.Close()

	// Should eventually connect after 2 failed attempts
	assert.Eventually(t, func() bool {
		return client.GetConnectionStatus() == StateConnected
	}, 5*time.Second, 500*time.Millisecond, "should eventually connect")

	// Test message after reconnection
	err := client.Send(Message{Type: testMessageType})
	require.NoError(t, err, "should send after reconnection")

	assert.Eventually(t, func() bool {
		return received.Load() == 1
	}, 2*time.Second, 100*time.Millisecond, "should receive message after reconnect")
}

func TestWSClientSendBuffer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup blocking test server
	var blockConn atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockConn.Load() {
			time.Sleep(30 * time.Second) // Simulate long disconnect
			return
		}
		echoHandler(w, r)
	}))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL:  wsURL,
		ClientID:   "buffer-client",
		Headers:    testHeaders,
		MaxRetries: 2,
	}, nil)

	client.Start()
	defer client.Close()

	// Fill the send buffer
	for i := 0; i < maxSendBuffer; i++ {
		err := client.Send(Message{Type: fmt.Sprintf("msg-%d", i)})
		require.NoError(t, err, "should fill send buffer")
	}

	// This send should trigger buffer full protection
	start := time.Now()
	err := client.Send(Message{Type: "overflow"})
	require.Error(t, err, "should get buffer full error")
	assert.WithinDuration(t, time.Now(), start, rateLimit+50*time.Millisecond, "should respect rate limit")

	// Verify oldest message gets dropped when requeuing
	blockConn.Store(true)
	time.Sleep(1 * time.Second) // Allow time for connection drop

	// These sends should trigger requeue with drops
	for i := 0; i < 10; i++ {
		client.Send(Message{Type: fmt.Sprintf("requeue-%d", i)})
	}

	// Verify buffer contains newest messages
	assert.Eventually(t, func() bool {
		lastMsgType := fmt.Sprintf("requeue-%d", 9)
		_, exists := client.handlers[lastMsgType]
		return exists
	}, 2*time.Second, 100*time.Millisecond, "should retain newest messages")
}

func TestWSClientPanicRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ts := httptest.NewServer(http.HandlerFunc(echoHandler))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL: wsURL,
		ClientID:  "panic-client",
		Headers:   testHeaders,
	}, nil)

	// Setup panic handlers
	client.RegisterHandler("send-panic", func(msg *Message) {
		panic("send panic")
	})
	client.RegisterHandler("receive-panic", func(msg *Message) {
		panic("receive panic")
	})

	client.Start()
	defer client.Close()

	// Trigger panics
	client.Send(Message{Type: "send-panic"})
	client.Send(Message{Type: "receive-panic"})

	// Verify client continues operating
	assert.Eventually(t, func() bool {
		return client.GetConnectionStatus() == StateConnected
	}, 2*time.Second, 100*time.Millisecond, "should recover from panics")

	// Test normal operation after panic
	var received atomic.Int32
	client.RegisterHandler("normal-msg", func(msg *Message) {
		received.Add(1)
	})
	client.Send(Message{Type: "normal-msg"})

	assert.Eventually(t, func() bool {
		return received.Load() == 1
	}, 2*time.Second, 100*time.Millisecond, "should handle messages after panic")
}

func TestWSClientBackoffBehavior(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup failing test server
	var connectCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectCount.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL:  wsURL,
		ClientID:   "backoff-client",
		Headers:    testHeaders,
		MaxRetries: 3,
	}, nil)

	client.Start()
	defer client.Close()

	// Verify backoff timing
	attempts := make([]time.Time, 0, 3)
	assert.Eventually(t, func() bool {
		attempts = append(attempts, time.Now())
		return connectCount.Load() >= 3
	}, 5*time.Second, 10*time.Millisecond)

	// Check delays between attempts
	for i := 1; i < len(attempts); i++ {
		delay := attempts[i].Sub(attempts[i-1])
		minDelay := initialBackoff * time.Duration(1<<uint(i-1)) / 2
		maxDelay := initialBackoff * time.Duration(1<<uint(i-1)) * 3 / 2
		assert.True(t, delay >= minDelay && delay <= maxDelay,
			"attempt %d delay %v not in expected range [%v-%v]",
			i, delay, minDelay, maxDelay)
	}
}

func TestWSClientConnectionStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Setup controllable test server
	var allowConnect atomic.Bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowConnect.Load() {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		echoHandler(w, r)
	}))
	defer ts.Close()
	wsURL := "ws" + ts.URL[4:]

	client := NewWSClient(ctx, Config{
		ServerURL: wsURL,
		ClientID:  "state-client",
		Headers:   testHeaders,
	}, nil)

	client.Start()
	defer client.Close()

	// Initial state should be disconnected
	assert.Equal(t, StateDisconnected, client.GetConnectionStatus())

	// Enable connections and verify state
	allowConnect.Store(true)
	assert.Eventually(t, func() bool {
		return client.GetConnectionStatus() == StateConnected
	}, 2*time.Second, 100*time.Millisecond)

	// Disable connections and verify state
	allowConnect.Store(false)
	client.closeConnection() // Force disconnect
	assert.Eventually(t, func() bool {
		return client.GetConnectionStatus() == StateDisconnected
	}, 2*time.Second, 100*time.Millisecond)
}

func echoHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"pbs"},
	})
	if err != nil {
		log.Printf("WebSocket accept error: %v", err)
		return
	}
	defer conn.Close(websocket.StatusInternalError, "server shutdown")

	ctx := context.Background()
	for {
		var msg Message
		err := wsjson.Read(ctx, conn, &msg)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Printf("Read error: %v", err)
			}
			return
		}

		// Echo message back with server timestamp
		msg.Time = time.Now()
		if err := wsjson.Write(ctx, conn, msg); err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}
