package websockets

import (
	"context"
	"fmt"
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
	// Setup server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	// Create test HTTP server
	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	// Convert http to ws
	wsURL := "ws" + ts.URL[4:]

	// Test cases
	tests := []struct {
		name     string
		clientID string
		msgs     []Message
	}{
		{
			name:     "Single client basic messaging",
			clientID: "client1",
			msgs: []Message{
				{Type: "test", Content: "hello"},
				{Type: "test", Content: "world"},
			},
		},
		{
			name:     "Message broadcasting",
			clientID: "client2",
			msgs: []Message{
				{Type: "broadcast", Content: "broadcast message"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create client
			client, err := NewWSClient(ctx, Config{
				ServerURL: wsURL,
				ClientID:  tt.clientID,
				Headers: http.Header{
					"X-Client-ID": []string{tt.clientID},
				},
			})
			require.NoError(t, err)

			// Connect client
			err = client.Connect()
			require.NoError(t, err)
			defer client.Close()

			// Start client
			client.Start()

			// Setup message receiving
			receivedMsgs := make(chan Message, len(tt.msgs))
			client.RegisterHandler("test", func(msg *Message) {
				receivedMsgs <- *msg
			})
			client.RegisterHandler("broadcast", func(msg *Message) {
				receivedMsgs <- *msg
			})

			// Send test messages
			for _, msg := range tt.msgs {
				client.Send(msg)
			}

			// Verify messages are received
			for i := 0; i < len(tt.msgs); i++ {
				select {
				case received := <-receivedMsgs:
					assert.Equal(t, tt.clientID, received.ClientID)
					assert.NotEmpty(t, received.Time)
					assert.Contains(t, []string{"test", "broadcast"}, received.Type)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for message")
				}
			}
		})
	}
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

	// Wait for disconnection to be detected
	time.Sleep(100 * time.Millisecond)
	assert.False(t, client.IsConnected)

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
	defer client.Close()

	client.Start()

	// Send messages rapidly
	start := time.Now()
	for i := 0; i < 20; i++ {
		client.Send(Message{
			Type:    "test",
			Content: fmt.Sprintf("message %d", i),
		})
	}
	duration := time.Since(start)

	// Verify rate limiting worked (should take at least 2 seconds for 20 messages with 10 per second limit)
	assert.Greater(t, duration.Seconds(), 2.0)
}
