package arpc

import "testing"

func TestAgentsManagerIsOnline(t *testing.T) {
	sm := NewAgentsManager()
	if sm.IsOnline("missing") {
		t.Fatal("expected offline for unknown client id")
	}
	sm.quicSessions.Set("quic", &QuicPipe{})
	if !sm.IsOnline("quic") {
		t.Fatal("expected online via QUIC session")
	}
	sm.sessions.Set("stream", &StreamPipe{})
	if !sm.IsOnline("stream") {
		t.Fatal("expected online via stream session")
	}
	if sm.IsOnline("missing") {
		t.Fatal("expected offline for unknown client id")
	}
}
