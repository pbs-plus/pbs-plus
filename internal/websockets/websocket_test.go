//go:build linux

package websockets

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"sync"
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

func TestCPUOnDisconnect(t *testing.T) {
	_, wsURL := setupTestServer(t)

	const numClients = 100
	clients := make([]*WSClient, numClients)

	// Create and connect clients
	for i := 0; i < numClients; i++ {
		client, err := createTestClient(fmt.Sprintf("cpu-test-%d", i), wsURL)
		require.NoError(t, err)

		err = client.Connect(context.Background())
		require.NoError(t, err)

		clients[i] = client
	}

	t.Cleanup(func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	})

	// Baseline measurement
	baselineFile, err := os.Create("cpu-baseline.prof")
	require.NoError(t, err)
	defer os.Remove(baselineFile.Name())

	err = pprof.StartCPUProfile(baselineFile)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	pprof.StopCPUProfile()

	baselineStats, err := os.ReadFile(baselineFile.Name())
	require.NoError(t, err)

	// Disconnect measurement
	for _, client := range clients {
		client.Close()
	}

	peakFile, err := os.Create("cpu-peak.prof")
	require.NoError(t, err)
	defer os.Remove(peakFile.Name())

	err = pprof.StartCPUProfile(peakFile)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	pprof.StopCPUProfile()

	peakStats, err := os.ReadFile(peakFile.Name())
	require.NoError(t, err)

	// Compare measurements
	baselineCPURate := float64(len(baselineStats)) / 5
	peakCPURate := float64(len(peakStats)) / 5

	t.Logf("Baseline CPU: %f bytes/s", baselineCPURate)
	t.Logf("Peak CPU: %f bytes/s", peakCPURate)
	assert.Less(t, peakCPURate/baselineCPURate, 1.2, "CPU usage more than 1.2x during disconnects")
}
