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

func setupTestServer(t *testing.T) (*Server, string, context.CancelFunc) {
	initializeLogger()

	ctx, cancel := context.WithCancel(context.Background())
	server := NewServer(ctx)
	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	t.Cleanup(func() {
		ts.Close()
		cancel()
	})

	wsURL := "ws" + ts.URL[4:]
	return server, wsURL, cancel
}

func createTestClient(ctx context.Context, url, id string) (*WSClient, error) {
	return NewWSClient(ctx, Config{
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
	server, wsURL, cancel := setupTestServer(t)
	defer cancel()

	t.Run("Single client basic messaging", func(t *testing.T) {
		// Create message channels for test synchronization
		serverReceived := make(chan Message, 1)
		clientReceived := make(chan Message, 1)

		// Register server handler
		server.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
			select {
			case serverReceived <- *msg:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		// Create and connect client
		client, err := createTestClient(context.Background(), wsURL, "test-client")
		require.NoError(t, err)
		defer client.Close()

		err = client.Connect(context.Background())
		require.NoError(t, err)

		// Register client handler
		client.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
			select {
			case clientReceived <- *msg:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		// Test client -> server
		testMsg := Message{Type: "test", Content: "hello"}
		err = client.Send(context.Background(), testMsg)
		require.NoError(t, err)

		select {
		case received := <-serverReceived:
			assert.Equal(t, testMsg.Type, received.Type)
			assert.Equal(t, testMsg.Content, received.Content)
			assert.Equal(t, client.config.ClientID, received.ClientID)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for server to receive message")
		}

		// Test server -> client
		err = server.SendToClient(client.config.ClientID, testMsg)
		require.NoError(t, err)

		select {
		case received := <-clientReceived:
			assert.Equal(t, testMsg.Type, received.Type)
			assert.Equal(t, testMsg.Content, received.Content)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for client to receive message")
		}
	})

	t.Run("Multiple clients messaging", func(t *testing.T) {
		numClients := 5
		messageCount := 10
		received := make([]chan Message, numClients)
		clients := make([]*WSClient, numClients)

		// Create and connect clients
		for i := 0; i < numClients; i++ {
			received[i] = make(chan Message, messageCount)
			clientID := fmt.Sprintf("test-client-%d", i)

			client, err := createTestClient(context.Background(), wsURL, clientID)
			require.NoError(t, err)

			err = client.Connect(context.Background())
			require.NoError(t, err)

			idx := i // Capture loop variable
			client.RegisterHandler("test", func(ctx context.Context, msg *Message) error {
				select {
				case received[idx] <- *msg:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})

			clients[i] = client
		}

		// Send messages from each client
		for i, client := range clients {
			for j := 0; j < messageCount; j++ {
				msg := Message{
					Type:    "test",
					Content: fmt.Sprintf("message-%d-%d", i, j),
				}
				err := client.Send(context.Background(), msg)
				require.NoError(t, err)
			}
		}

		// Verify each client received its messages
		for i := 0; i < numClients; i++ {
			receivedCount := 0
			for j := 0; j < messageCount; j++ {
				select {
				case <-received[i]:
					receivedCount++
				case <-time.After(2 * time.Second):
					t.Fatalf("timeout waiting for client %d to receive message %d", i, j)
				}
			}
			assert.Equal(t, messageCount, receivedCount, "client %d did not receive all messages", i)
		}

		// Cleanup
		for _, client := range clients {
			client.Close()
		}
	})
}

func TestClientReconnection(t *testing.T) {
	_, wsURL, cancel := setupTestServer(t)
	defer cancel()

	client, err := createTestClient(context.Background(), wsURL, "reconnect-test")
	require.NoError(t, err)
	defer client.Close()

	// Test initial connection
	err = client.Connect(context.Background())
	require.NoError(t, err)
	assert.True(t, client.isConnected.Load())

	// Force disconnect
	client.conn.Close(websocket.StatusGoingAway, "testing reconnection")

	// Verify disconnect detection
	require.Eventually(t, func() bool {
		return !client.GetConnectionStatus()
	}, 5*time.Second, 100*time.Millisecond, "Connection should have been marked as disconnected")

	// Verify reconnection
	require.Eventually(t, func() bool {
		return client.GetConnectionStatus()
	}, 5*time.Second, 100*time.Millisecond, "Connection should have been re-established")

	// Verify functionality after reconnection
	testMsg := Message{Type: "test", Content: "after-reconnect"}
	err = client.Send(context.Background(), testMsg)
	require.NoError(t, err, "Should be able to send messages after reconnection")
}

func TestCPUOnDisconnect(t *testing.T) {
	_, wsURL, cancel := setupTestServer(t)
	defer cancel()

	numClients := 100
	clients := make([]*WSClient, numClients)
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()

	// Connect clients
	for i := 0; i < numClients; i++ {
		client, err := createTestClient(context.Background(), wsURL, fmt.Sprintf("cpu-test-%d", i))
		require.NoError(t, err)

		err = client.Connect(context.Background())
		require.NoError(t, err)

		clients[i] = client
	}

	// Measure baseline CPU usage
	baselineFile, err := os.Create("cpu-baseline.prof")
	require.NoError(t, err)
	defer os.Remove(baselineFile.Name())

	err = pprof.StartCPUProfile(baselineFile)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	pprof.StopCPUProfile()

	baselineStats, err := os.ReadFile(baselineFile.Name())
	require.NoError(t, err)

	// Disconnect all clients
	time.Sleep(time.Second)
	for _, client := range clients {
		client.Close()
	}

	// Measure CPU during disconnection handling
	peakFile, err := os.Create("cpu-peak.prof")
	require.NoError(t, err)
	defer os.Remove(peakFile.Name())

	err = pprof.StartCPUProfile(peakFile)
	require.NoError(t, err)
	time.Sleep(5 * time.Second)
	pprof.StopCPUProfile()

	peakStats, err := os.ReadFile(peakFile.Name())
	require.NoError(t, err)

	// Compare CPU usage
	baselineCPURate := float64(len(baselineStats)) / 5
	peakCPURate := float64(len(peakStats)) / 5

	t.Logf("Baseline CPU: %f bytes/s", baselineCPURate)
	t.Logf("Peak CPU: %f bytes/s", peakCPURate)
	assert.Less(t, peakCPURate/baselineCPURate, 1.2, "CPU usage more than 1.2x during disconnects")
}
