//go:build linux

package dovecot

import (
	"bytes"
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestStageBackupReportsStagingProgress(t *testing.T) {
	tempDir := t.TempDir()
	logPath := filepath.Join(tempDir, "calls.log")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", filepath.Join(tempDir, "dovecot.conf"))
	t.Setenv("DOVECOT_TEST_CREATE_MAIL", "1")

	program := filepath.Join(tempDir, "doveadm")
	writeDovecotTestProgram(t, program)
	target := coredb.Target{
		Type:         coredb.TargetTypeDovecot,
		DatabaseHost: "mail.example.com",
		DatabasePort: 24245,
	}

	var log bytes.Buffer
	staged, err := StageBackup(context.Background(), tempDir, target, "pa55word", BackupOptions{
		Username:  "alice@example.com",
		LogWriter: &log,
	}, Client{Program: program, Version: "2.4.1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staged.Cleanup() })

	logged := log.String()
	for _, want := range []string{
		"Dovecot staging: preparing mailbox data; PBS snapshot upload has not started",
		"Dovecot staging: 1 mailbox users selected; syncing them one at a time before PBS upload",
		"Dovecot staging: user 1/1 \"alice@example.com\"; PBS upload not started",
		"Dovecot staging: completed user 1/1 \"alice@example.com\"; 1 succeeded, 0 failed",
		"Dovecot staging complete: 1 users succeeded, 0 failed",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("staging log missing %q:\n%s", want, logged)
		}
	}
	if strings.Contains(logged, "pa55word") {
		t.Fatal("staging log leaked the doveadm password")
	}
}

func TestStageBackupServerScopeReportsUserListPhase(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	logPath := filepath.Join(tempDir, "calls.log")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", filepath.Join(tempDir, "dovecot.conf"))
	t.Setenv("DOVECOT_TEST_CREATE_MAIL", "1")

	program := filepath.Join(tempDir, "doveadm")
	writeDovecotTestProgram(t, program)

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, password, ok := r.BasicAuth()
		if !ok || user != apiUser || password != "pa55word" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if _, err := io.WriteString(w, `[["doveadmResponse",{"alice@example.com":{"uid":"1"},"carol@example.com":{"uid":"2"}},"pbs-plus"]]`); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	host, port := splitTestHostPort(t, server.URL)
	target := coredb.Target{
		Type:                  coredb.TargetTypeDovecot,
		DatabaseHost:          host,
		DatabasePort:          24245,
		DovecotAPIPort:        port,
		DatabaseCACertificate: caPath,
	}

	var log bytes.Buffer
	staged, err := StageBackup(context.Background(), tempDir, target, "pa55word", BackupOptions{
		Scope:     ScopeServer,
		LogWriter: &log,
	}, Client{Program: program, Version: "2.4.1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = staged.Cleanup() })

	logged := log.String()
	for _, want := range []string{
		"Dovecot staging: requesting the mailbox user list; PBS upload not started",
		"Dovecot staging: 2 mailbox users selected",
		"Dovecot staging: user 2/2",
		"Dovecot staging complete: 2 users succeeded, 0 failed",
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("server-scope staging log missing %q:\n%s", want, logged)
		}
	}
}
