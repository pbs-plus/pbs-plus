//go:build linux

package websockets

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}
	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	t.Log("Starting integration test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Run()
	}()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	t.Run("Single client basic messaging", func(t *testing.T) {
		clientID := "test-client"

		// Create WebSocket connection directly for testing
		conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-PBS-Agent": []string{clientID}},
		})
		require.NoError(t, err)
		defer conn.Close(websocket.StatusNormalClosure, "test complete")

		handler, cleanup := server.RegisterHandler()
		defer cleanup()

		// Send message from client
		sentMsg := Message{Type: "test", Content: "hello"}
		require.NoError(t, wsjson.Write(ctx, conn, sentMsg))

		// Verify server received message
		select {
		case rcvdMsg := <-handler:
			assert.Equal(t, clientID, rcvdMsg.ClientID)
			assert.Equal(t, sentMsg.Type, rcvdMsg.Type)
			assert.Equal(t, sentMsg.Content, rcvdMsg.Content)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		// Send message to client
		serverMsg := Message{Type: "server-msg", Content: "response"}
		require.NoError(t, server.SendToClient(clientID, serverMsg))

		// Verify client received message
		var received Message
		require.NoError(t, wsjson.Read(ctx, conn, &received))
		assert.Equal(t, serverMsg.Type, received.Type)
		assert.Equal(t, serverMsg.Content, received.Content)
	})

	select {
	case err := <-serverErr:
		require.NoError(t, err)
	default:
	}
}

func TestClientReconnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Run()
	}()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	clientID := "reconnect-test"

	// Initial connection
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"X-PBS-Agent": []string{clientID}},
	})
	require.NoError(t, err)

	// Force disconnect
	conn.Close(websocket.StatusGoingAway, "testing reconnection")

	// Attempt reconnection
	var newConn *websocket.Conn
	require.Eventually(t, func() bool {
		newConn, _, err = websocket.Dial(ctx, wsURL, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-PBS-Agent": []string{clientID}},
		})
		return err == nil
	}, 5*time.Second, 500*time.Millisecond, "reconnection failed")
	defer newConn.Close(websocket.StatusNormalClosure, "test complete")

	// Verify server state
	assert.True(t, server.IsClientConnected(clientID))

	select {
	case err := <-serverErr:
		require.NoError(t, err)
	default:
	}
}

func TestCPUOnDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := NewServer(ctx)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Run()
	}()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	// Create load profile helper
	profileCPU := func(duration time.Duration) []byte {
		f, err := os.CreateTemp("", "cpuprofile")
		require.NoError(t, err)
		defer os.Remove(f.Name())

		pprof.StartCPUProfile(f)
		time.Sleep(duration)
		pprof.StopCPUProfile()

		data, err := os.ReadFile(f.Name())
		require.NoError(t, err)
		return data
	}

	// Baseline measurement
	baseline := profileCPU(2 * time.Second)

	// Create clients
	conns := make([]*websocket.Conn, 100)
	for i := range conns {
		conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
			HTTPHeader: http.Header{"X-PBS-Agent": []string{fmt.Sprintf("load-%d", i)}},
		})
		require.NoError(t, err)
		conns[i] = conn
	}

	// Peak measurement
	peak := profileCPU(2 * time.Second)

	// Cleanup clients
	for _, conn := range conns {
		conn.Close(websocket.StatusNormalClosure, "test complete")
	}

	// Calculate metrics
	baselineSize := len(baseline)
	peakSize := len(peak)
	ratio := float64(peakSize) / float64(baselineSize)

	t.Logf("CPU profile ratio: %.2f", ratio)
	assert.Less(t, ratio, 1.5, "CPU usage increased more than expected")

	select {
	case err := <-serverErr:
		require.NoError(t, err)
	default:
	}
}
