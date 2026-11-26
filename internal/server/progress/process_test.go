//go:build linux

package progress

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestParseProcessIO(t *testing.T) {
	io, err := parseProcessIO("rchar: 1024\nwchar: 512\nsyscr: 2\n")
	if err != nil {
		t.Fatal(err)
	}
	if io.read != 1024 || io.write != 512 {
		t.Fatalf("parsed = %+v", io)
	}
	if _, err := parseProcessIO("syscr: 2\n"); err == nil {
		t.Fatal("missing counters must error")
	}
}

func TestProcessLineReportsRatesAndIdle(t *testing.T) {
	line := processLine("phase", 30*time.Second,
		processIO{read: 3 << 20, write: 1 << 20},
		processIO{read: 1 << 20, write: 0},
		15*time.Second, 60*time.Second)
	for _, want := range []string{"phase; elapsed 30s", "read 3.00 MiB (0.13 MiB/s)", "write 1.00 MiB (0.07 MiB/s)", "no process read/write activity observed for 1m0s"} {
		if !strings.Contains(line, want) {
			t.Errorf("line %q missing %q", line, want)
		}
	}
	if strings.Contains(processLine("phase", 0, processIO{}, processIO{}, 0, 0), "no process") {
		t.Error("idle note must only appear once activity has stopped")
	}
}

func TestWatchSelfProcessReportsRates(t *testing.T) {
	lines := make(chan string, 8)
	stop := watch(context.Background(), os.Getpid(), "staging phase", func(line string) {
		select {
		case lines <- line:
		default:
		}
	}, 30*time.Millisecond)
	defer stop()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case line := <-lines:
			if strings.Contains(line, "elapsed") {
				if !strings.Contains(line, "process read ") || !strings.Contains(line, "write ") {
					t.Fatalf("rate line missing counters: %q", line)
				}
				stop()
				return
			}
		case <-deadline:
			stop()
			t.Fatal("watch never reported a rate line")
		}
	}
}

func TestWatchWithoutProcessStatesWaiting(t *testing.T) {
	lines := make(chan string, 8)
	ctx := t.Context()
	stop := watch(ctx, 0, "user list phase", func(line string) {
		select {
		case lines <- line:
		default:
		}
	}, 20*time.Millisecond)
	defer stop()
	deadline := time.After(5 * time.Second)
	sawFirst, sawWaiting := false, false
	for !(sawFirst && sawWaiting) {
		select {
		case line := <-lines:
			if line == "user list phase" {
				sawFirst = true
			}
			if strings.Contains(line, "still waiting for this stage to finish") {
				sawWaiting = true
			}
		case <-deadline:
			stop()
			t.Fatalf("watch lines missing milestones: first=%v waiting=%v", sawFirst, sawWaiting)
		}
	}
	stop()
	stop()
}
