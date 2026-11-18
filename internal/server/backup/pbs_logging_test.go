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

func TestIsClientWarning(t *testing.T) {
	for line, want := range map[string]bool{
		`warning: stale file handle encountered while reading: "a/b.ts"`:                                           true,
		`2026-11-18T14:48:25-05:00: warning: file size shrunk while reading: "x", file will be padded with zeros!`: true,
		"pbs-plus: WARNING: unreadable file cut short":                                                             true,
		`  warning: stale file handle encountered while reading: "c"`:                                              true,
		`skipping "reports/warning: notes.txt"`:                                                                    false,
		"bkp.ppxar: had to backup 390.8 GiB of 391.769 GiB":                                                        false,
		"no warnings here": false,
	} {
		if got := isClientWarning(line); got != want {
			t.Errorf("isClientWarning(%q) = %v, want %v", line, got, want)
		}
	}
}

func TestMergePBSLogsCountsLowercaseClientWarnings(t *testing.T) {
	dir := t.TempDir()
	proxyLog := filepath.Join(dir, "proxy.log")
	writeLines(t, proxyLog, []string{
		"2026-11-18T12:18:37-05:00: starting backup",
		"2026-11-18T14:48:42-05:00: TASK OK",
	}, 0660)

	jobID := "pbsplus-test-" + strings.ReplaceAll(t.Name(), "/", "-")
	defer func() {
		_ = os.Remove(filepath.Join(os.TempDir(), "job-"+jobID+"-stdout"))
	}()
	logger := log.WithScope(log.Scope{JobID: jobID})
	if err := logger.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	clientLog := logger.JobLogPath()
	writeLines(t, clientLog, []string{
		"Starting backup: host/bkp-ingest-station-Root",
		`warning: stale file handle encountered while reading: "Data/Video/T1636.ts"`,
		`warning: stale file handle encountered while reading: "test.gpf"`,
		`warning: file size shrunk while reading: "big.bin", file will be padded with zeros!`,
		"bkp.ppxar: had to backup 390.8 GiB of 391.769 GiB",
		"Duration: 8991.48s",
		"End Time: Wed Nov 18 14:48:28 2026",
	}, 0666)

	succeeded, _, warnings, err := mergePBSLogs(proxyLog, clientLog, logger, true, nil, "")
	if err != nil {
		t.Fatalf("mergePBSLogs: %v", err)
	}
	if !succeeded || warnings != 3 {
		t.Fatalf("succeeded=%v warnings=%d, want true and 3", succeeded, warnings)
	}
	merged := readLines(t, proxyLog)
	if last := merged[len(merged)-1]; !strings.HasSuffix(last, "TASK WARNINGS: 3") {
		t.Errorf("final line = %q, want TASK WARNINGS: 3", last)
	}
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
		"--- MariaDB log starts here ---",
		"mariadb-dump: dumping grants",
		"client: starting",
		"pbs-plus: WARNING: owner metadata was unavailable",
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

	for attempt := range 2 {
		succeeded, cancelled, warnings, err := mergePBSLogs(proxyLog, realClientPath, logger, true, nil, "")
		if err != nil {
			t.Fatalf("mergePBSLogs: %v", err)
		}
		if !succeeded {
			t.Error("succeeded = false, want true")
		}
		if cancelled {
			t.Error("cancelled = true, want false")
		}
		if warnings != 1 {
			t.Errorf("warnings = %d, want 1", warnings)
		}
		if attempt == 0 {
			clientLines = append(clientLines, "post-backup script: done")
			writeLines(t, realClientPath, clientLines, 0666)
		}
	}

	mergedProxy := readLines(t, proxyLog)

	terminalCount := 0
	for _, line := range mergedProxy {
		if strings.Contains(line, ": TASK ") {
			terminalCount++
		}
	}
	if terminalCount != 1 || !strings.HasSuffix(mergedProxy[len(mergedProxy)-1], "TASK WARNINGS: 1") {
		t.Errorf("proxy log terminal lines = %d, want one PBS Plus TASK WARNINGS: 1 line; got:\n%s", terminalCount, strings.Join(mergedProxy, "\n"))
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
	if !contains(mergedProxy, "post-backup script: done") {
		t.Errorf("proxy log missing client content added after the first merge; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	if !contains(mergedProxy, "--- MariaDB log starts here ---") || !contains(mergedProxy, "mariadb-dump: dumping grants") {
		t.Errorf("proxy log missing MariaDB content; got:\n%s", strings.Join(mergedProxy, "\n"))
	}
	separatorCount := 0
	databaseSeparatorCount := 0
	clientSeparatorIndex := -1
	databaseSeparatorIndex := -1
	for index, line := range mergedProxy {
		if line == "--- proxmox-backup-client log starts here ---" {
			separatorCount++
			clientSeparatorIndex = index
		}
		if line == "--- MariaDB log starts here ---" {
			databaseSeparatorCount++
			databaseSeparatorIndex = index
		}
	}
	if separatorCount != 1 {
		t.Errorf("proxy log separators = %d, want one", separatorCount)
	}
	if databaseSeparatorCount != 1 {
		t.Errorf("database log separators = %d, want one", databaseSeparatorCount)
	}
	if databaseSeparatorIndex <= clientSeparatorIndex {
		t.Errorf("database log index = %d, want after client log index %d", databaseSeparatorIndex, clientSeparatorIndex)
	}
	if !contains(mergedProxy, "starting backup") {
		t.Errorf("proxy log lost original proxy content; got:\n%s", strings.Join(mergedProxy, "\n"))
	}

	afterClient := readLines(t, realClientPath)
	if contains(afterClient, "TASK OK") {
		t.Errorf("client log was rewritten with merge output (regression); got:\n%s", strings.Join(afterClient, "\n"))
	}
}

func TestMergePBSLogsReplaysTheCapturedFailureAfterRotation(t *testing.T) {
	dir := t.TempDir()
	proxyLog := filepath.Join(dir, "proxy.log")

	writeLines(t, proxyLog, []string{
		"2026-10-31T21:39:36-04:00: starting backup",
		"2026-10-31T22:22:54-04:00: backup failed: connection error: not connected",
	}, 0660)

	jobID := "pbsplus-test-rotated-" + strings.ReplaceAll(t.Name(), "/", "-")
	defer func() {
		_ = os.Remove(filepath.Join(os.TempDir(), "job-"+jobID+"-stdout"))
	}()
	logger := log.WithScope(log.Scope{JobID: jobID})
	if err := logger.FlushJobLog(); err != nil {
		t.Fatalf("flush job log: %v", err)
	}
	realClientPath := logger.JobLogPath()
	writeLines(t, realClientPath, []string{}, 0666)

	captured := `Error: upload failed: update atime failed for chunk/file "/phoenix/.chunks/2b8c/2b8cafd066ee" - EPERM: Operation not permitted`
	if _, _, _, err := mergePBSLogs(proxyLog, realClientPath, logger, true, nil, captured); err != nil {
		t.Fatalf("mergePBSLogs: %v", err)
	}

	merged := strings.Join(readLines(t, proxyLog), "\n")
	if !strings.Contains(merged, "update atime failed") {
		t.Fatalf("rotated client log lost the captured failure:\n%s", merged)
	}
	if !strings.Contains(merged, "TASK ERROR: Error: upload failed: update atime failed") {
		t.Fatalf("task result must name the root cause:\n%s", merged)
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

	succeeded, cancelled, _, err := mergePBSLogs(proxyLog, realClientPath, logger, true, nil, "")
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
