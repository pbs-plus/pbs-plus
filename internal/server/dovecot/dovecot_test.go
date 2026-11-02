//go:build linux

package dovecot

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestSelectClient(t *testing.T) {
	tempDir := t.TempDir()
	binDir := filepath.Join(tempDir, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"doveadm", "dovecot"} {
		program := "#!/bin/sh\nexit 0\n"
		if name == "dovecot" {
			program = "#!/bin/sh\nprintf '2.4.1\\n'\n"
		}
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(program), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	client, err := selectClient(context.Background(), []string{binDir}, []string{tempDir})
	if err != nil {
		t.Fatal(err)
	}
	if client.Program != filepath.Join(binDir, "doveadm") || client.Version != "2.4.1" {
		t.Fatalf("client = %#v", client)
	}

	if err := os.WriteFile(filepath.Join(binDir, "dovecot"), []byte("#!/bin/sh\nprintf '2.3.21.1\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := selectClient(context.Background(), []string{binDir}, []string{tempDir}); err == nil {
		t.Fatal("Dovecot 2.3 client was selected")
	}
}

func TestStageBackupAndRestoreBackup(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tempDir, "calls.log")
	configCopyPath := filepath.Join(tempDir, "dovecot.conf")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", configCopyPath)
	t.Setenv("DOVECOT_TEST_CREATE_MAIL", "1")

	program := filepath.Join(tempDir, "doveadm")
	writeDovecotTestProgram(t, program)
	target := coredb.Target{
		Type:                  coredb.TargetTypeDovecot,
		DatabaseHost:          "mail.example.com",
		DatabasePort:          24245,
		DatabaseCACertificate: caPath,
	}
	client := Client{Program: program, Version: "2.4.1"}

	staged, err := StageBackup(context.Background(), tempDir, target, "pa55word", BackupOptions{
		Username: "alice@example.com",
		Mailbox:  "Archive",
	}, client)
	if err != nil {
		t.Fatal(err)
	}
	root := staged.root
	t.Cleanup(func() { _ = staged.Cleanup() })

	if staged.Manifest.Version != ManifestVersion || staged.Manifest.Scope != ScopeUser || staged.Manifest.Mailbox != "Archive" || staged.Manifest.ClientVersion != "2.4.1" || staged.Manifest.CreatedAt.IsZero() {
		t.Fatalf("manifest = %#v", staged.Manifest)
	}
	userDir := UserDirName("alice@example.com")
	if len(staged.Manifest.Users) != 1 || staged.Manifest.Users[0].Username != "alice@example.com" || staged.Manifest.Users[0].Dir != userDir {
		t.Fatalf("manifest users = %#v", staged.Manifest.Users)
	}
	if _, err := os.Stat(filepath.Join(staged.ArchiveDir, usersDirName, userDir, mailDirName, "message")); err != nil {
		t.Fatalf("staged mail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "secrets")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("backup secrets remain: %v", err)
	}

	config, err := os.ReadFile(configCopyPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"doveadm_password = pa55word",
		"ssl_client_require_valid_cert = yes",
		"mail_driver = maildir",
		"sieve_script personal {",
	} {
		if !strings.Contains(string(config), want) {
			t.Errorf("client config missing %q:\n%s", want, config)
		}
	}

	calls := readDovecotTestCalls(t, logPath)
	wantBackup := []string{
		"USER=alice@example.com",
		"ARG=-c",
		"ARG=backup",
		"ARG=--no-userdb-lookup",
		"ARG=-m",
		"ARG=Archive",
		"ARG=-R",
		"ARG=tcps:mail.example.com:24245",
	}
	assertDovecotTestCall(t, calls, 0, wantBackup)
	if strings.Contains(strings.Join(calls[0], "\n"), "pa55word") {
		t.Fatal("password appeared in command arguments")
	}

	if err := RestoreBackup(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{
		SourceUsername:      "alice@example.com",
		DestinationUsername: "bob@example.com",
	}, client); err != nil {
		t.Fatal(err)
	}
	calls = readDovecotTestCalls(t, logPath)
	assertDovecotTestCall(t, calls, 1, []string{
		"USER=bob@example.com",
		"ARG=-c",
		"ARG=sync",
		"ARG=--no-userdb-lookup",
		"ARG=-1",
		"ARG=-m",
		"ARG=Archive",
		"ARG=tcps:mail.example.com:24245",
	})
	assertDovecotTestCall(t, calls, 2, []string{
		"USER=bob@example.com",
		"ARG=-c",
		"ARG=sync",
		"ARG=--no-userdb-lookup",
		"ARG=-1",
		"ARG=-m",
		"ARG=Archive",
		"ARG=tcps:mail.example.com:24245",
	})

	if err := RestoreBackup(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{
		SourceUsername:  "alice@example.com",
		ReplaceExisting: true,
	}, client); err != nil {
		t.Fatal(err)
	}
	calls = readDovecotTestCalls(t, logPath)
	assertDovecotTestCall(t, calls, 3, []string{
		"USER=alice@example.com",
		"ARG=-c",
		"ARG=backup",
		"ARG=--no-userdb-lookup",
		"ARG=-m",
		"ARG=Archive",
		"ARG=tcps:mail.example.com:24245",
	})

	if err := RestoreBackup(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{SourceUsername: "other@example.com"}, client); err == nil {
		t.Fatal("source username mismatch succeeded")
	}
	if err := RestoreBackup(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{SourceUsername: "alice@example.com", SourceMailbox: "INBOX"}, client); err == nil {
		t.Fatal("mailbox mismatch succeeded")
	}
	if got := len(readDovecotTestCalls(t, logPath)); got != 4 {
		t.Fatalf("invalid restore invoked doveadm: %d calls", got)
	}

	if err := staged.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging root remains: %v", err)
	}
}

func TestLoadManifestAndRestoreValidation(t *testing.T) {
	tempDir := t.TempDir()
	manifest := Manifest{
		Version: ManifestVersion,
		Scope:   ScopeUser,
		Mailbox: "Archive",
		Users:   []ManifestUser{{Username: "alice@example.com"}},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(tempDir, mailDirName)); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{Type: coredb.TargetTypeDovecot, DatabaseHost: "mail.example.com", DatabasePort: 24245, DatabaseCACertificate: caPath}
	if err := RestoreBackup(context.Background(), tempDir, target, "secret", RestoreOptions{SourceUsername: "alice@example.com"}, Client{Program: "/bin/false", Version: "2.4.1"}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("symlink restore error = %v", err)
	}
	if err := RestoreBackup(context.Background(), tempDir, target, "secret", RestoreOptions{SourceUsername: "carol@example.com"}, Client{Program: "/bin/false", Version: "2.4.1"}); err == nil || !strings.Contains(err.Error(), "does not contain user") {
		t.Fatalf("unknown user restore error = %v", err)
	}

	manifest.Version += 2
	data, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tempDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(tempDir); err == nil || !strings.Contains(err.Error(), "unsupported Dovecot manifest version") {
		t.Fatalf("manifest version error = %v", err)
	}
}

func TestWriteClientConfig(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "source-ca.pem")
	if err := os.WriteFile(caPath, []byte("certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	mailDir := filepath.Join(tempDir, "mail")
	if err := os.Mkdir(mailDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath, err := writeClientConfig(tempDir, mailDir, caPath, "pa55word")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{configPath, filepath.Join(tempDir, "ca.pem")} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Errorf("%s mode = %o", path, got)
		}
	}
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"dovecot_config_version = 2.4.0",
		"doveadm_password = pa55word",
		"ssl_client_ca_file = " + filepath.Join(tempDir, "ca.pem"),
		"ssl_client_require_valid_cert = yes",
		"mail_path = " + mailDir,
		"first_valid_uid = 0",
		"sieve_script personal {",
	} {
		if !strings.Contains(string(config), want) {
			t.Errorf("config missing %q:\n%s", want, config)
		}
	}
	for _, reject := range []string{"bad\npassword", "pass word", "pa\"ss", "pa#ss", "pa{ss"} {
		if _, err := passwordSetting(reject); err == nil {
			t.Errorf("password %q was accepted", reject)
		}
	}
}

func TestWriteClientConfigSystemCA(t *testing.T) {
	var systemBundle string
	for _, candidate := range systemCABundlePaths {
		if _, err := os.Stat(candidate); err == nil {
			systemBundle = candidate
			break
		}
	}
	tempDir := t.TempDir()
	mailDir := filepath.Join(tempDir, "mail")
	if err := os.Mkdir(mailDir, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath, err := writeClientConfig(tempDir, mailDir, "", "pa55word")
	if systemBundle == "" {
		if err == nil {
			t.Fatal("empty CA path was accepted without a system CA bundle")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(systemBundle)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(tempDir, "ca.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("ca.pem does not match %s", systemBundle)
	}
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), "ssl_client_require_valid_cert = yes") {
		t.Errorf("config dropped certificate validation:\n%s", config)
	}
}

func TestValidateRemoteValues(t *testing.T) {
	tests := []struct {
		name     string
		validate func() error
	}{
		{name: "empty username", validate: func() error { return validateUsername("") }},
		{name: "username newline", validate: func() error { return validateUsername("alice\nadmin") }},
		{name: "username carriage return", validate: func() error { return validateUsername("alice\radmin") }},
		{name: "username nul", validate: func() error { return validateUsername("alice\x00admin") }},
		{name: "mailbox newline", validate: func() error { return validateMailbox("Archive\nOther") }},
		{name: "mailbox nul", validate: func() error { return validateMailbox("Archive\x00Other") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.validate(); err == nil {
				t.Fatal("value was accepted")
			}
		})
	}
}

func TestSupportedVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
	}{
		{name: "Dovecot 2.3", version: "2.3.21.1", want: false},
		{name: "Dovecot 2.4", version: "2.4.1 (abcdef)", want: true},
		{name: "Dovecot 3", version: "3.0.0", want: true},
		{name: "empty", version: "", want: false},
		{name: "invalid", version: "Dovecot", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := supportedVersion(tt.version); got != tt.want {
				t.Fatalf("supportedVersion(%q) = %t, want %t", tt.version, got, tt.want)
			}
		})
	}
}

func writeDovecotTestProgram(t *testing.T, path string) {
	t.Helper()
	const program = `#!/bin/sh
set -eu
{
	printf 'USER=%s\n' "${USER-}"
	for arg do
		printf 'ARG=%s\n' "$arg"
	done
	printf 'END\n'
} >> "$DOVECOT_TEST_LOG"
config=
previous=
for arg do
	if [ "$previous" = "-c" ]; then
		config=$arg
	fi
	previous=$arg
done
cp "$config" "$DOVECOT_TEST_CONFIG"
if [ -n "${DOVECOT_TEST_FAIL_USER:-}" ] && [ "${DOVECOT_TEST_FAIL_USER}" = "${USER-}" ]; then
	exit 75
fi
if [ "${DOVECOT_TEST_CREATE_MAIL:-}" = "1" ]; then
	mail_dir=
	while IFS= read -r line; do
		case "$line" in
			"mail_path = "*) mail_dir=${line#mail_path = } ;;
		esac
	done < "$config"
	printf 'message body\n' > "$mail_dir/message"
fi
`
	if err := os.WriteFile(path, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
}

func readDovecotTestCalls(t *testing.T, path string) [][]string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	blocks := strings.Split(strings.TrimSpace(string(data)), "\nEND\n")
	calls := make([][]string, 0, len(blocks))
	for _, block := range blocks {
		block = strings.TrimSuffix(block, "\nEND")
		calls = append(calls, strings.Split(block, "\n"))
	}
	return calls
}

func assertDovecotTestCall(t *testing.T, calls [][]string, index int, want []string) {
	t.Helper()
	if len(calls) <= index {
		t.Fatalf("call %d missing: %#v", index, calls)
	}
	got := slices.Clone(calls[index])
	if len(got) < 3 || got[1] != "ARG=-c" {
		t.Fatalf("call %d = %#v", index, got)
	}
	got = append(got[:2], got[3:]...)
	if !slices.Equal(got, want) {
		t.Fatalf("call %d = %#v, want %#v", index, got, want)
	}
}

func TestStageBackupServerScope(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	logPath := filepath.Join(tempDir, "calls.log")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", filepath.Join(tempDir, "dovecot.conf"))
	t.Setenv("DOVECOT_TEST_CREATE_MAIL", "1")
	t.Setenv("DOVECOT_TEST_FAIL_USER", "carol@example.com")

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

	staged, err := StageBackup(context.Background(), tempDir, target, "pa55word", BackupOptions{Scope: ScopeServer}, Client{Program: program, Version: "2.4.1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := staged.Cleanup(); err != nil {
			t.Error(err)
		}
	})

	if staged.Manifest.Scope != ScopeServer || len(staged.Manifest.Users) != 1 || staged.Manifest.Users[0].Username != "alice@example.com" {
		t.Fatalf("manifest users = %#v", staged.Manifest.Users)
	}
	if !slices.Equal(staged.Manifest.Failed, []string{"carol@example.com"}) {
		t.Fatalf("manifest failed = %#v", staged.Manifest.Failed)
	}
	if _, err := os.Stat(filepath.Join(staged.ArchiveDir, usersDirName, UserDirName("alice@example.com"), mailDirName, "message")); err != nil {
		t.Fatalf("staged mail: %v", err)
	}
	if _, err := os.Stat(filepath.Join(staged.ArchiveDir, usersDirName, UserDirName("carol@example.com"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed user staging remains: %v", err)
	}
	if _, err := LoadManifest(staged.ArchiveDir); err != nil {
		t.Fatalf("load manifest: %v", err)
	}
}

func TestStageBackupServerScopeRequiresAPIPort(t *testing.T) {
	tempDir := t.TempDir()
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{Type: coredb.TargetTypeDovecot, DatabaseHost: "mail.example.com", DatabasePort: 24245, DatabaseCACertificate: caPath}
	_, err := StageBackup(context.Background(), tempDir, target, "pa55word", BackupOptions{Scope: ScopeServer}, Client{Program: "/bin/false", Version: "2.4.1"})
	if err == nil || !strings.Contains(err.Error(), "doveadm HTTP API port") {
		t.Fatalf("server scope without API port = %v", err)
	}
}

func TestRestoreBackupLegacyManifest(t *testing.T) {
	tempDir := t.TempDir()
	archiveDir := filepath.Join(tempDir, "archive")
	if err := os.MkdirAll(filepath.Join(archiveDir, mailDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(Manifest{Version: 1, Username: "alice@example.com", ClientVersion: "2.4.1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tempDir, "calls.log")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", filepath.Join(tempDir, "dovecot.conf"))
	program := filepath.Join(tempDir, "doveadm")
	writeDovecotTestProgram(t, program)

	target := coredb.Target{Type: coredb.TargetTypeDovecot, DatabaseHost: "mail.example.com", DatabasePort: 24245, DatabaseCACertificate: caPath}
	if err := RestoreBackup(context.Background(), archiveDir, target, "pa55word", RestoreOptions{}, Client{Program: program, Version: "2.4.1"}); err != nil {
		t.Fatal(err)
	}
	assertDovecotTestCall(t, readDovecotTestCalls(t, logPath), 0, []string{
		"USER=alice@example.com",
		"ARG=-c",
		"ARG=sync",
		"ARG=--no-userdb-lookup",
		"ARG=-1",
		"ARG=tcps:mail.example.com:24245",
	})
}

func TestRestoreBackupFlattenedUserArchive(t *testing.T) {
	tempDir := t.TempDir()
	archiveDir := filepath.Join(tempDir, "archive")
	user := ManifestUser{Username: "alice@example.com", Dir: UserDirName("alice@example.com")}
	if err := os.MkdirAll(filepath.Join(archiveDir, user.Dir, mailDirName), 0o700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(Manifest{Version: ManifestVersion, Scope: ScopeUser, Users: []ManifestUser{user}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(tempDir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("test ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(tempDir, "calls.log")
	t.Setenv("DOVECOT_TEST_LOG", logPath)
	t.Setenv("DOVECOT_TEST_CONFIG", filepath.Join(tempDir, "dovecot.conf"))
	program := filepath.Join(tempDir, "doveadm")
	writeDovecotTestProgram(t, program)

	target := coredb.Target{Type: coredb.TargetTypeDovecot, DatabaseHost: "mail.example.com", DatabasePort: 24245, DatabaseCACertificate: caPath}
	if err := RestoreBackup(context.Background(), archiveDir, target, "pa55word", RestoreOptions{SourceUsername: user.Username}, Client{Program: program, Version: "2.4.1"}); err != nil {
		t.Fatal(err)
	}
	assertDovecotTestCall(t, readDovecotTestCalls(t, logPath), 0, []string{
		"USER=alice@example.com",
		"ARG=-c",
		"ARG=sync",
		"ARG=--no-userdb-lookup",
		"ARG=-1",
		"ARG=tcps:mail.example.com:24245",
	})
}

func TestBuildSelection(t *testing.T) {
	mailDir := t.TempDir()
	sentDir := filepath.Join(mailDir, ".Sent", "cur")
	if err := os.MkdirAll(sentDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"1700000000.M1.mail:2,S", "1700000001.M2.mail:2,S"} {
		if err := os.WriteFile(filepath.Join(sentDir, name), []byte("body"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	root, mailbox, err := buildSelection(t.TempDir(), mailDir, "Sent", "Recovered/Sent", []string{".Sent/cur/1700000000.M1.mail:2,S"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	if mailbox != "Recovered/Sent" {
		t.Fatalf("mailbox = %q", mailbox)
	}
	if _, err := os.Stat(filepath.Join(root, mailDirName, ".Recovered.Sent", "cur", "1700000000.M1.mail:2,S")); err != nil {
		t.Fatalf("selected message: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, mailDirName, ".Recovered.Sent", "cur", "1700000001.M2.mail:2,S")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected message was copied: %v", err)
	}

	whole, mailbox, err := buildSelection(t.TempDir(), mailDir, "Sent", "Recovered", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(whole); err != nil {
			t.Error(err)
		}
	})
	entries, err := os.ReadDir(filepath.Join(whole, mailDirName, ".Recovered", "cur"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("copied %d messages", len(entries))
	}

	if _, _, err := buildSelection(t.TempDir(), mailDir, "Sent", "Recovered", []string{"../../etc/passwd"}); err == nil {
		t.Fatal("message outside the backup was accepted")
	}
	if _, _, err := buildSelection(t.TempDir(), mailDir, "Trash", "", nil); err == nil {
		t.Fatal("empty mailbox selection was accepted")
	}
}

func TestUserDirName(t *testing.T) {
	first := UserDirName("alice@example.com")
	if first != UserDirName("alice@example.com") {
		t.Fatal("user directory is not deterministic")
	}
	if first == UserDirName("bob@example.com") {
		t.Fatal("different users share a directory")
	}
	for _, username := range []string{"../escape", ".hidden", "with space", strings.Repeat("long", 40)} {
		dir := UserDirName(username)
		if err := validateUserDir(dir); err != nil {
			t.Fatalf("UserDirName(%q) = %q: %v", username, dir, err)
		}
	}
}

func TestParseUserList(t *testing.T) {
	listed, err := parseUserList([]byte(`[["doveadmResponse",{"userList":["alice@example.com","dave@example.com"]},"pbs-plus"]]`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(listed, []string{"alice@example.com", "dave@example.com"}) {
		t.Fatalf("listed users = %#v", listed)
	}

	keyed, err := parseUserList([]byte(`[["doveadmResponse",{"bob@example.com":{},"alice@example.com":{}},"pbs-plus"]]`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keyed, []string{"alice@example.com", "bob@example.com"}) {
		t.Fatalf("keyed users = %#v", keyed)
	}

	rows, err := parseUserList([]byte(`[["doveadmResponse",[{"username":"alice@example.com"},{"username":"alice@example.com"}],"pbs-plus"]]`))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rows, []string{"alice@example.com"}) {
		t.Fatalf("row users = %#v", rows)
	}

	if _, err := parseUserList([]byte(`[["error",{"exitCode":68,"type":"exitCode"},"pbs-plus"]]`)); err == nil {
		t.Fatal("doveadm error response was accepted")
	}
}

func splitTestHostPort(t *testing.T, rawURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatal(err)
	}
	return parsed.Hostname(), port
}
