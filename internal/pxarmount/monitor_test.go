package pxarmount

import (
	"bufio"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestCommitHubClosesWatchersOnError(t *testing.T) {
	hub, err := newCommitHub(filepath.Join(t.TempDir(), "commit.sock"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(hub.close)

	hub.startJob()
	conn, err := net.Dial("unix", hub.sockPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatalf("read initial progress: %v", scanner.Err())
	}

	hub.broadcast("ERR commit failed")

	if !hub.ended.Load() {
		t.Fatal("error did not mark commit as ended")
	}
	if got := hub.watchers.Len(); got != 0 {
		t.Fatalf("watchers after error = %d, want 0", got)
	}
}
