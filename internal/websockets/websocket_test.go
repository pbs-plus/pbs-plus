//go:build linux

package websockets

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	t.Log("Starting integration test")

	err := syslog.InitializeLogger()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %s", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	t.Run("Single client basic messaging", func(t *testing.T) {
		client, err := NewWSClient(ctx, Config{
			ServerURL: wsURL,
			ClientID:  "test-client",
			Headers: http.Header{
				"X-PBS-Agent": []string{"test-client"},
			},
		}, &tls.Config{
			InsecureSkipVerify: true,
		})
		require.NoError(t, err)

		err = client.Connect()
		require.NoError(t, err)

		messageReceived := make(chan struct{})
		client.RegisterHandler("test", func(msg *Message) {
			t.Logf("Received message: %+v", msg)
			close(messageReceived)
		})

		client.Start()

		clientMessage, cleanUp := server.RegisterHandler()

		msg := Message{Type: "test", Content: "hello"}
		err = client.Send(msg)
		require.NoError(t, err)

		select {
		case rcvdMsg := <-clientMessage:
			assert.Equal(t, msg.Type, rcvdMsg.Type)
			assert.Equal(t, msg.Content, rcvdMsg.Content)
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		cleanUp()

		err = server.SendToClient(client.ClientID, msg)
		require.NoError(t, err)

		select {
		case <-messageReceived:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message")
		}

		t.Log("Finished test, closing client.")
		client.Close()
	})
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
			"X-PBS-Agent": []string{"reconnect-test"},
		},
	}, &tls.Config{
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	// Test initial connection
	err = client.Connect()
	require.NoError(t, err)
	assert.True(t, client.IsConnected)

	client.Start()

	// Force disconnect
	client.conn.Close(websocket.StatusGoingAway, "testing reconnection")

	// Wait for disconnection to be detected with timeout
	disconnected := false
	for i := 0; i < 50; i++ {
		if !client.GetConnectionStatus() {
			disconnected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	assert.True(t, disconnected, "Connection should have been marked as disconnected")

	connected := false
	for i := 0; i < 50; i++ {
		if client.GetConnectionStatus() {
			connected = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Test reconnection
	assert.True(t, connected)

	t.Log("Finished test, closing client.")
	client.Close()
}

func TestCPUOnDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := NewServer(ctx)
	go server.Run()

	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]

	clients := make([]*WSClient, 100)
	for i := 0; i < 100; i++ {
		client, err := NewWSClient(ctx, Config{
			ServerURL: wsURL,
			ClientID:  fmt.Sprintf("cpu-test-%d", i),
			Headers: http.Header{
				"X-PBS-Agent": []string{fmt.Sprintf("cpu-test-%d", i)},
			},
		}, &tls.Config{
			InsecureSkipVerify: true,
		})
		require.NoError(t, err)

		err = client.Connect()
		require.NoError(t, err)
		client.Start()
		clients[i] = client
	}

	time.Sleep(time.Second)

	mu := sync.Mutex{}
	var cpuUsage []float64
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var stats runtime.MemStats
				runtime.ReadMemStats(&stats)
				cpuTime := time.Duration(stats.PauseTotalNs)

				mu.Lock()
				cpuUsage = append(cpuUsage, float64(cpuTime.Nanoseconds()))
				mu.Unlock()
			}
		}
	}()

	for _, client := range clients {
		client.Close()
	}

	time.Sleep(2 * time.Second)
	close(done)

	mu.Lock()
	measurements := make([]float64, len(cpuUsage))
	copy(measurements, cpuUsage)
	mu.Unlock()

	var maxSpike float64
	for i := 1; i < len(measurements); i++ {
		spike := measurements[i] - measurements[i-1]
		if spike > maxSpike {
			maxSpike = spike
		}
	}

	// Calculate reasonable spike threshold based on baseline CPU usage
	var baselineSpike float64
	for i := 1; i < len(measurements)/2; i++ { // Use first half of measurements as baseline
		spike := measurements[i] - measurements[i-1]
		baselineSpike = math.Max(baselineSpike, spike)
	}

	t.Logf("maxSpike: %f", maxSpike)
	t.Logf("baselineSpike: %f", baselineSpike)

	assert.Less(t, maxSpike, baselineSpike*1.2, "CPU spike during disconnects exceeded 1.2x baseline usage")
}
