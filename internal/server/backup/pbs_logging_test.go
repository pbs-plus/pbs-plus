//go:build linux

package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func writeLines(t *testing.T, path string, lines []string, perm os.FileMode) {
	t.Helper()
	var sb strings.Builder
	for _, l := range lines {
		sb.WriteString(l)
		sb.WriteByte('\n')
	}
	if err := os.WriteFile(path, []byte(sb.String()), perm); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.Split(strings.TrimRight(string(b), "\n"), "\n")
}

func contains(lines []string, needle string) bool {
	for _, l := range lines {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}

func TestMergePBSLogsRewritesProxyLogNotClientLog(t *testing.T) {
	dir := t.TempDir()
	proxyLog := filepath.Join(dir, "proxy.log")
	clientLog := filepath.Join(dir, "client.log")

	proxyLines := []string{
		"2026-06-25T20:00:00-04:00: starting backup",
		"upload_chunk done: 1234 bytes",
		"POST /dynamic_chunk",
		"2026-06-25T20:01:00-04:00: TASK OK",
	}
	writeLines(t, proxyLog, proxyLines, 0660)

	clientLines := []string{
		"client: starting",
		"Duration: 1.00s",
		"End Time: 2026-06-25T20:01:00-04:00",
	}
	writeLines(t, clientLog, clientLines, 0660)

	jobID := "pbsplus-test-" + strings.ReplaceAll(t.Name(), "/", "-")
	defer func() {
		_ = os.Remove(filepath.Join(os.TempDir(), "job-"+jobID+"-stdout"))
	}()

	logger := log.WithScope(log.Scope{JobID: jobID})
	if err := logger.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	realClientPath := logger.JobLogPath()
	if realClientPath == "" {
		t.Fatal("job log path is empty")
	}
	writeLines(t, realClientPath, clientLines, 0666)

	succeeded, cancelled, warnings, err := mergePBSLogs(proxyLog, realClientPath, logger, true, nil)
	if err != nil {
		t.Fatalf("mergePBSLogs: %v", err)
	}
	if !succeeded {
		t.Errorf("succeeded = false, want true")
	}
	if cancelled {
		t.Errorf("cancelled = true, want false")
	}
	if warnings != 0 {
		t.Errorf("warnings = %d, want 0", warnings)
	}

	mergedProxy := readLines(t, proxyLog)

	if !contains(mergedProxy, "TASK OK") {
		t.Errorf("proxy log missing TASK OK status line; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	if contains(mergedProxy, "upload_chunk done:") {
		t.Errorf("proxy log still contains junk line; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	if !contains(mergedProxy, "--- proxmox-backup-client log starts here ---") {
		t.Errorf("proxy log missing client log separator; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	if !contains(mergedProxy, "client: starting") {
		t.Errorf("proxy log missing merged client content; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	if !contains(mergedProxy, "starting backup") {
		t.Errorf("proxy log lost original proxy content; got:\n%s", strings.Join(mergedProxy, "\n"))
	}

	afterClient := readLines(t, realClientPath)
	if contains(afterClient, "TASK OK") {
		t.Errorf("client log was rewritten with merge output (regression); got:\n%s", strings.Join(afterClient, "\n"))
	}
}

func TestMergePBSLogsIncompleteYieldsErrorStatus(t *testing.T) {
	dir := t.TempDir()
	proxyLog := filepath.Join(dir, "proxy.log")

	writeLines(t, proxyLog, []string{
		"2026-06-25T20:00:00-04:00: starting backup",
		"something happened",
	}, 0660)

	jobID := "pbsplus-test-incomplete-" + strings.ReplaceAll(t.Name(), "/", "-")
	defer func() {
		_ = os.Remove(filepath.Join(os.TempDir(), "job-"+jobID+"-stdout"))
	}()
	logger := log.WithScope(log.Scope{JobID: jobID})
	if err := logger.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	realClientPath := logger.JobLogPath()
	writeLines(t, realClientPath, []string{"client: no completion markers here"}, 0666)

	succeeded, cancelled, _, err := mergePBSLogs(proxyLog, realClientPath, logger, true, nil)
	if err != nil {
		t.Fatalf("mergePBSLogs: %v", err)
	}
	if succeeded {
		t.Errorf("succeeded = true for incomplete log, want false")
	}
	if !cancelled {
		t.Errorf("cancelled = false for incomplete log, want true")
	}

	mergedProxy := readLines(t, proxyLog)
	if !contains(mergedProxy, "TASK ERROR") {
		t.Errorf("proxy log missing TASK ERROR for incomplete backup; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
}
