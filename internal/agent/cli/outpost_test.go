package cli

import (
	"io"
	"net"
	"testing"
	"time"
)

func TestBridgeCopiesBothWaysAndClosesBoth(t *testing.T) {
	client, agentClientSide := net.Pipe()
	agentTunnelSide, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		_ = bridge(agentClientSide, agentTunnelSide)
		close(done)
	}()

	go func() { _, _ = client.Write([]byte("request")) }()
	buf := make([]byte, 7)
	if _, err := io.ReadFull(server, buf); err != nil || string(buf) != "request" {
		t.Fatalf("server read %q, %v", buf, err)
	}
	go func() { _, _ = server.Write([]byte("reply")) }()
	buf = buf[:5]
	if _, err := io.ReadFull(client, buf); err != nil || string(buf) != "reply" {
		t.Fatalf("client read %q, %v", buf, err)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("bridge did not finish after the client closed")
	}
	if _, err := server.Read(buf); err == nil {
		t.Fatal("tunnel side left open after the client closed")
	}
}
