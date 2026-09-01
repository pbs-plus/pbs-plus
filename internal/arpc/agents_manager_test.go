package arpc

import "testing"

func TestAgentsManagerIsOnline(t *testing.T) {
	sm := NewAgentsManager()
	if sm.IsOnline("missing") {
		t.Fatal("expected offline for unknown client id")
	}
	sm.quicSessions.Set("quic", &QuicPipe{})
	if sm.IsOnline("quic") {
		t.Fatal("expected offline for a session with no live connection")
	}
	sm.sessions.Set("stream", &StreamPipe{})
	if sm.IsOnline("stream") {
		t.Fatal("expected offline for a session with no live connection")
	}
	if sm.IsOnline("missing") {
		t.Fatal("expected offline for unknown client id")
	}
}
