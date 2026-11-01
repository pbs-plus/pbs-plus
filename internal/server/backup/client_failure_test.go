//go:build linux

package backup

import (
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

const (
	atimeLine = `2026-10-31T22:22:54-04:00: Error: upload failed: update atime failed for chunk/file "/phoenix/.chunks/2b8c/2b8c53522ab4b18f0110e487ce7cf3859abf01191f560dba8cac738f461bc8a8" - EPERM: Operation not permitted`
	panicLine = `thread 'tokio-runtime-worker' (8729) panicked at pbs-client/src/chunk_stream.rs:127:49:`
)

func TestFirstClientFailurePrefersTheRootCause(t *testing.T) {
	chunk := []byte(strings.Join([]string{
		"processed 1.203 TiB in 1h 20m 0.1s, uploaded 85.025 MiB",
		panicLine,
		"note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace",
		atimeLine,
	}, "\n"))

	got := firstClientFailure(chunk)
	if !strings.Contains(got, "update atime failed") {
		t.Fatalf("want the atime root cause, got %q", got)
	}
	if !strings.Contains(got, "EPERM: Operation not permitted") {
		t.Fatalf("want the full line including EPERM, got %q", got)
	}
}

func TestFirstClientFailureCapturesAPanicWithoutAnAtimeLine(t *testing.T) {
	chunk := []byte("processed 48.053 GiB in 1h 20m, uploaded 88.877 MiB\n" + panicLine + "\n")

	got := firstClientFailure(chunk)
	if !strings.Contains(got, "panicked at pbs-client/src/chunk_stream.rs") {
		t.Fatalf("want the panic line, got %q", got)
	}
}

func TestFirstClientFailureIgnoresHealthyProgress(t *testing.T) {
	chunk := []byte(strings.Join([]string{
		"processed 2.179 TiB in 3h 15m 0.2s, uploaded 323.424 GiB",
		"upload_chunk done: 2112843 bytes",
		"successfully added chunk",
	}, "\n"))

	if got := firstClientFailure(chunk); got != "" {
		t.Fatalf("healthy output must record no failure, got %q", got)
	}
}

func TestRecordClientFailureKeepsTheFirstLineOnly(t *testing.T) {
	b := &backupJob{}
	b.recordClientFailure(atimeLine)
	b.recordClientFailure(panicLine)

	if !strings.Contains(b.clientFailureLine(), "update atime failed") {
		t.Fatalf("the first recorded failure must win, got %q", b.clientFailureLine())
	}
}

func TestReportLinesSurfaceTheClientFailure(t *testing.T) {
	joined := strings.Join(
		reportLines(agentJob(), jobs.ErrUnexpected, 1700000000, 1700003600, atimeLine), "\n")

	if !strings.Contains(joined, "Client error: ") {
		t.Fatalf("a recorded client failure must reach the task log:\n%s", joined)
	}
	if !strings.Contains(joined, "update atime failed") {
		t.Fatalf("the task log must name the root cause:\n%s", joined)
	}
}

func TestReportLinesOmitTheClientFailureWhenItDuplicatesDetail(t *testing.T) {
	err := jobs.ErrUnexpected
	joined := strings.Join(
		reportLines(agentJob(), err, 1700000000, 1700003600, err.Error()), "\n")

	if strings.Contains(joined, "Client error: ") {
		t.Fatalf("a client failure already shown as Detail must not repeat:\n%s", joined)
	}
}
