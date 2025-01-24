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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := NewServer(ctx)
	go server.Run()
	ts := httptest.NewServer(http.HandlerFunc(server.ServeWS))
	defer ts.Close()

	wsURL := "ws" + ts.URL[4:]
	clients := make([]*WSClient, 100)
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()

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

	baselineFile, err := os.Create("cpu-baseline.prof")
	require.NoError(t, err)
	defer os.Remove(baselineFile.Name())

	err = pprof.StartCPUProfile(baselineFile)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)

	pprof.StopCPUProfile()
	baselineStats, err := os.ReadFile(baselineFile.Name())
	require.NoError(t, err)

	time.Sleep(time.Second)

	for _, client := range clients {
		client.Close()
	}

	peakFile, err := os.Create("cpu-peak.prof")
	require.NoError(t, err)
	defer os.Remove(peakFile.Name())

	err = pprof.StartCPUProfile(peakFile)
	require.NoError(t, err)

	time.Sleep(time.Second * 5)

	pprof.StopCPUProfile()
	peakStats, err := os.ReadFile(peakFile.Name())
	require.NoError(t, err)

	baselineCPURate := float64(len(baselineStats)) / 5
	peakCPURate := float64(len(peakStats)) / 5

	t.Logf("Baseline CPU: %f bytes/s", baselineCPURate)
	t.Logf("Peak CPU: %f bytes/s", peakCPURate)
	assert.Less(t, peakCPURate/baselineCPURate, 1.2, "CPU usage more than 1.2x during disconnects")
}
