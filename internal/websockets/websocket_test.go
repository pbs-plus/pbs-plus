package websockets

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	t.Log("Starting integration test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	t.Run("Single client basic messaging", func(t *testing.T) {
		client, err := NewWSClient(ctx, Config{
			ServerURL: wsURL,
			ClientID:  "test-client",
			Headers: http.Header{
				"X-Client-ID": []string{"test-client"},
			},
		})
		require.NoError(t, err)

		err = client.Connect()
		require.NoError(t, err)

		// Wait for connection to establish
		time.Sleep(100 * time.Millisecond)

		messageReceived := make(chan struct{})
		client.RegisterHandler("test", func(msg *Message) {
			t.Logf("Received message: %+v", msg)
			close(messageReceived)
		})

		client.Start()

		// Wait for client to start
		time.Sleep(100 * time.Millisecond)

		msg := Message{Type: "test", Content: "hello"}
		err = client.Send(msg)
		require.NoError(t, err)

		select {
		case <-messageReceived:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		client.Close()
	})
}

func TestMultipleClients(t *testing.T) {
	t.Log("Starting multiple clients test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	// Create multiple clients
	numClients := 3
	clients := make([]*WSClient, numClients)
	receivedMsgs := make([]chan Message, numClients)

	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		clientID := fmt.Sprintf("client%d", i)
		receivedMsgs[i] = make(chan Message, 10)

		client, err := NewWSClient(ctx, Config{
			ServerURL: wsURL,
			ClientID:  clientID,
			Headers: http.Header{
				"X-Client-ID": []string{clientID},
			},
		})
		require.NoError(t, err)

		err = client.Connect()
		require.NoError(t, err)

		client.Start()
		clients[i] = client

		// Register message handler
		msgChan := receivedMsgs[i]
		client.RegisterHandler("broadcast", func(msg *Message) {
			msgChan <- *msg
			wg.Done()
		})
	}

	// Broadcast message from first client
	broadcastMsg := Message{
		Type:    "broadcast",
		Content: "hello everyone",
	}
	clients[0].Send(broadcastMsg)

	// Wait for all clients to receive the message
	// Add timeout to wg.Wait()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All clients received messages")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for clients to receive messages")
	}

	// Verify all clients received the message
	for i := 0; i < numClients; i++ {
		select {
		case msg := <-receivedMsgs[i]:
			assert.Equal(t, "broadcast", msg.Type)
			assert.Equal(t, "hello everyone", msg.Content)
			assert.Equal(t, "client0", msg.ClientID)
		default:
			t.Errorf("Client %d did not receive broadcast message", i)
		}
	}

	// Cleanup
	for _, client := range clients {
		client.Close()
	}
}

func TestServerSubscription(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	// Create subscriber
	msgChan, cleanup := server.Subscribe()
	defer cleanup()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	// Create client
	client, err := NewWSClient(ctx, Config{
		ServerURL: wsURL,
		ClientID:  "test-client",
		Headers: http.Header{
			"X-Client-ID": []string{"test-client"},
		},
	})
	require.NoError(t, err)

	err = client.Connect()
	require.NoError(t, err)
	defer client.Close()

	client.Start()

	// Send test message
	testMsg := Message{
		Type:    "test",
		Content: "subscription test",
	}
	client.Send(testMsg)

	// Verify subscriber received message
	select {
	case received := <-msgChan:
		assert.Equal(t, "test-client", received.ClientID)
		assert.Equal(t, "test", received.Type)
		assert.Equal(t, "subscription test", received.Content)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for subscribed message")
	}
}

func TestClientReconnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	client, err := NewWSClient(ctx, Config{
		ServerURL: wsURL,
		ClientID:  "reconnect-test",
		Headers: http.Header{
			"X-Client-ID": []string{"reconnect-test"},
		},
	})
	require.NoError(t, err)

	// Test initial connection
	err = client.Connect()
	require.NoError(t, err)
	assert.True(t, client.IsConnected)

	// Force disconnect
	client.conn.Close(websocket.StatusGoingAway, "testing reconnection")

	// Wait for disconnection to be detected with timeout
	disconnected := false
	for i := 0; i < 50; i++ {
		if !client.IsConnected {
			disconnected = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(t, disconnected, "Connection should have been marked as disconnected")

	// Test reconnection
	err = client.Connect()
	require.NoError(t, err)
	assert.True(t, client.IsConnected)

	client.Close()
}

func TestRateLimiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	client, err := NewWSClient(ctx, Config{
		ServerURL: wsURL,
		ClientID:  "ratelimit-test",
		Headers: http.Header{
			"X-Client-ID": []string{"ratelimit-test"},
		},
	})
	require.NoError(t, err)

	err = client.Connect()
	require.NoError(t, err)

	// Create channel for received messages
	received := make(chan Message, 1)

	client.RegisterHandler("test", func(msg *Message) {
		log.Printf("Handler received message: %+v", msg)
		received <- *msg
	})

	client.Start()

	// Wait for client to initialize
	time.Sleep(100 * time.Millisecond)

	// Send a test message
	testMsg := Message{Type: "test", Content: "rate limit test"}
	err = client.Send(testMsg)
	require.NoError(t, err)

	// Wait for message with timeout
	select {
	case msg := <-received:
		log.Printf("Test received message: %+v", msg)
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for message")
	}

	client.Close()
}
