//go:build linux

package dovecot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
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

	if staged.Manifest.Version != ManifestVersion || staged.Manifest.Username != "alice@example.com" || staged.Manifest.Mailbox != "Archive" || staged.Manifest.ClientVersion != "2.4.1" || staged.Manifest.CreatedAt.IsZero() {
		t.Fatalf("manifest = %#v", staged.Manifest)
	}
	if _, err := os.Stat(filepath.Join(staged.ArchiveDir, mailDirName, "message")); err != nil {
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
	if err := RestoreBackup(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{SourceUsername: "alice@example.com", Mailbox: "INBOX"}, client); err == nil {
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
	manifest := Manifest{Version: ManifestVersion, Username: "alice@example.com", Mailbox: "Archive"}
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
	if err := RestoreBackup(context.Background(), tempDir, target, "secret", RestoreOptions{SourceUsername: manifest.Username}, Client{Program: "/bin/false", Version: "2.4.1"}); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("symlink restore error = %v", err)
	}

	manifest.Version++
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
