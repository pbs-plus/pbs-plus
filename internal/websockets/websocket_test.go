//go:build linux

package websockets

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var initializeLogger = sync.OnceFunc(func() {
	err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}
})

func setupTestServer(t *testing.T) (*Server, string) {
	initializeLogger()

	// Use background context for server
	server := NewServer(context.Background())
	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	t.Cleanup(func() {
		ts.Close()
	})

	wsURL := "ws" + ts.URL[4:]
	return server, wsURL
}

func createTestClient(id string, url string) (*WSClient, error) {
	return NewWSClient(context.Background(), Config{
		ServerURL: url,
		ClientID:  id,
		Headers: http.Header{
			"X-PBS-Agent": []string{id},
		},
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
}

func TestIntegration(t *testing.T) {
	server, wsURL := setupTestServer(t)

	t.Run("Single client basic messaging", func(t *testing.T) {
		serverMessages := make(chan Message, 10)
		clientMessages := make(chan Message, 10)

		// Register server handler
		server.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
			serverMessages <- *msg
			return nil
		})

		// Create and connect client
		client, err := createTestClient("test-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Connect(context.Background())
		require.NoError(t, err)

		// Register client handler
		client.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
			clientMessages <- *msg
			return nil
		})

		// Test client -> server
		msg := Message{Type: "test", Content: "hello"}
		err = client.Send(context.Background(), msg)
		require.NoError(t, err)

		// Wait for server to receive message
		select {
		case received := <-serverMessages:
			assert.Equal(t, msg.Type, received.Type)
			assert.Equal(t, msg.Content, received.Content)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for server to receive message")
		}

		// Test server -> client
		err = server.SendToClient(client.config.ClientID, msg)
		require.NoError(t, err)

		select {
		case received := <-clientMessages:
			assert.Equal(t, msg.Type, received.Type)
			assert.Equal(t, msg.Content, received.Content)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for client to receive message")
		}
	})

	t.Run("Multiple clients messaging", func(t *testing.T) {
		const numClients = 5
		const messagesPerClient = 10

		var wg sync.WaitGroup
		receivedMessages := make(chan Message, numClients*messagesPerClient)

		// Setup message tracking
		messageTracker := struct {
			sync.Mutex
			count int
		}{}

		// Register server handler
		server.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
			receivedMessages <- *msg
			messageTracker.Lock()
			messageTracker.count++
			messageTracker.Unlock()
			return nil
		})

		// Create and connect clients
		clients := make([]*WSClient, numClients)
		for i := 0; i < numClients; i++ {
			clientID := fmt.Sprintf("test-client-%d", i)
			client, err := createTestClient(clientID, wsURL)
			require.NoError(t, err)

			err = client.Connect(context.Background())
			require.NoError(t, err)

			clients[i] = client

			// Cleanup after test
			t.Cleanup(func() {
				client.Close()
			})
		}

		// Send messages from all clients
		wg.Add(numClients)
		for i, client := range clients {
			go func(idx int, c *WSClient) {
				defer wg.Done()
				for j := 0; j < messagesPerClient; j++ {
					msg := Message{
						Type:    "test",
						Content: fmt.Sprintf("message-%d-%d", idx, j),
					}
					err := c.Send(context.Background(), msg)
					require.NoError(t, err)
					time.Sleep(50 * time.Millisecond) // Prevent flooding
				}
			}(i, client)
		}

		// Wait for all sends to complete
		wg.Wait()

		// Wait for all messages to be received with timeout
		timeout := time.After(5 * time.Second)
		expectedMessages := numClients * messagesPerClient

		for {
			messageTracker.Lock()
			count := messageTracker.count
			messageTracker.Unlock()

			if count >= expectedMessages {
				break
			}

			select {
			case <-timeout:
				t.Fatalf("timeout waiting for messages. Got %d, expected %d", count, expectedMessages)
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	})
}

func TestClientReconnection(t *testing.T) {
	_, wsURL := setupTestServer(t)

	client, err := createTestClient("reconnect-test", wsURL)
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.Connect(context.Background())
	require.NoError(t, err)
	assert.True(t, client.isConnected.Load())

	// Force disconnect
	client.conn.Close(websocket.StatusGoingAway, "testing reconnection")

	// Wait for disconnect
	require.Eventually(t, func() bool {
		return !client.GetConnectionStatus()
	}, 5*time.Second, 100*time.Millisecond, "Connection should have been marked as disconnected")

	// Wait for reconnect
	require.Eventually(t, func() bool {
		return client.GetConnectionStatus()
	}, 5*time.Second, 100*time.Millisecond, "Connection should have been re-established")

	// Verify functionality after reconnection
	testMsg := Message{Type: "test", Content: "after-reconnect"}
	err = client.Send(context.Background(), testMsg)
	require.NoError(t, err, "Should be able to send messages after reconnection")
}

func TestMessageTypes(t *testing.T) {
	_, wsURL := setupTestServer(t)

	t.Run("Large message handling", func(t *testing.T) {
		client, err := createTestClient("large-msg-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Connect(context.Background())
		require.NoError(t, err)

		// Create a large message (1MB)
		largeContent := make([]byte, 1024*1024)
		for i := range largeContent {
			largeContent[i] = byte(i % 256)
		}

		msg := Message{
			Type:    "test-large",
			Content: string(largeContent),
		}

		err = client.Send(context.Background(), msg)
		require.NoError(t, err, "Should handle large messages")
	})

	t.Run("Rapid message sequence", func(t *testing.T) {
		client, err := createTestClient("rapid-msg-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Connect(context.Background())
		require.NoError(t, err)

		// Send 100 messages rapidly
		for i := 0; i < 100; i++ {
			msg := Message{
				Type:    "test-rapid",
				Content: fmt.Sprintf("rapid-message-%d", i),
			}
			err = client.Send(context.Background(), msg)
			require.NoError(t, err)
		}
	})
}

func TestConcurrentHandlers(t *testing.T) {
	server, wsURL := setupTestServer(t)

	t.Run("Multiple handlers per message type", func(t *testing.T) {
		client, err := createTestClient("multi-handler-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Connect(context.Background())
		require.NoError(t, err)

		handlerCalls := make(map[string]int)
		var handlerMutex sync.Mutex

		// Register multiple handlers for the same message type
		for i := 0; i < 3; i++ {
			handlerID := fmt.Sprintf("handler-%d", i)
			server.RegisterHandler("multi-handler", func(ctx context.Context, msg *Message) error {
				handlerMutex.Lock()
				handlerCalls[handlerID]++
				handlerMutex.Unlock()
				return nil
			})
		}

		// Send test message
		msg := Message{Type: "multi-handler", Content: "test"}
		err = client.Send(context.Background(), msg)
		require.NoError(t, err)

		// Wait and verify all handlers were called
		time.Sleep(1 * time.Second)

		handlerMutex.Lock()
		assert.Equal(t, 3, len(handlerCalls), "All handlers should have been called")
		handlerMutex.Unlock()
	})
}

func TestErrorHandling(t *testing.T) {
	server, wsURL := setupTestServer(t)

	t.Run("Invalid message handling", func(t *testing.T) {
		client, err := createTestClient("error-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Connect(context.Background())
		require.NoError(t, err)

		// Register handler that returns error
		server.RegisterHandler("error-test", func(ctx context.Context, msg *Message) error {
			return fmt.Errorf("intentional error")
		})

		msg := Message{Type: "error-test", Content: "test"}
		err = client.Send(context.Background(), msg)
		require.NoError(t, err)

		// Verify client is still connected after handler error
		time.Sleep(500 * time.Millisecond)
		assert.True(t, client.GetConnectionStatus())
	})

	t.Run("Context cancellation", func(t *testing.T) {
		client, err := createTestClient("context-client", wsURL)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		err = client.Connect(ctx)
		require.NoError(t, err)

		// Cancel context and verify connection handling
		cancel()
		time.Sleep(500 * time.Millisecond)
		assert.False(t, client.GetConnectionStatus())
	})
}

func TestStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	_, wsURL := setupTestServer(t)
	const (
		numClients        = 50
		messagesPerClient = 100
		messageSize       = 1024 // 1KB per message
	)

	t.Run("High load handling", func(t *testing.T) {
		var wg sync.WaitGroup
		successCount := atomic.Int32{}

		// Create and connect multiple clients
		clients := make([]*WSClient, numClients)
		for i := 0; i < numClients; i++ {
			client, err := createTestClient(fmt.Sprintf("stress-client-%d", i), wsURL)
			require.NoError(t, err)
			t.Cleanup(func() { client.Close() })

			err = client.Connect(context.Background())
			require.NoError(t, err)

			clients[i] = client

			// Register handler
			client.RegisterHandler("stress-test", func(ctx context.Context, msg *Message) error {
				successCount.Add(1)
				return nil
			})
		}

		// Generate random message content
		content := make([]byte, messageSize)
		rand.Read(content)

		// Send messages from all clients simultaneously
		wg.Add(numClients)
		for _, client := range clients {
			go func(c *WSClient) {
				defer wg.Done()
				for j := 0; j < messagesPerClient; j++ {
					msg := Message{
						Type:    "stress-test",
						Content: base64.StdEncoding.EncodeToString(content),
					}
					if err := c.Send(context.Background(), msg); err != nil {
						t.Logf("Error sending message: %v", err)
						return
					}
				}
			}(client)
		}

		wg.Wait()

		// Verify message handling
		expectedMessages := numClients * messagesPerClient
		require.Eventually(t, func() bool {
			return successCount.Load() == int32(expectedMessages)
		}, 30*time.Second, 100*time.Millisecond, "Not all messages were processed successfully")
	})
}
