//go:build linux

package dovecot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	ManifestVersion = 1
	manifestName    = "dovecot-manifest.json"
	mailDirName     = "mail"
)

type Client struct {
	Program string
	Version string
}

type BackupOptions struct {
	Username  string
	Mailbox   string
	LogWriter io.Writer
}

type RestoreOptions struct {
	SourceUsername      string
	DestinationUsername string
	Mailbox             string
	ReplaceExisting     bool
	LogWriter           io.Writer
}

type Manifest struct {
	Version       int       `json:"version"`
	Username      string    `json:"username"`
	Mailbox       string    `json:"mailbox,omitempty"`
	ClientVersion string    `json:"client_version"`
	CreatedAt     time.Time `json:"created_at"`
}

type StagedBackup struct {
	ArchiveDir string
	Manifest   Manifest
	root       string
}

func (s *StagedBackup) Cleanup() error {
	if s == nil || s.root == "" {
		return nil
	}
	err := os.RemoveAll(s.root)
	s.root = ""
	return err
}

func SelectClient(ctx context.Context, target coredb.Target) (Client, error) {
	dirs := []string{target.DatabaseDefaultClientDir}
	if target.DatabaseDefaultClientDir == "" {
		dirs = []string{"/usr/bin", "/usr/local/bin"}
	}
	return selectClient(ctx, dirs, []string{"/usr", "/usr/local", "/opt"})
}

func selectClient(ctx context.Context, dirs, trustedRoots []string) (Client, error) {
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		canonicalDir, err := filepath.EvalSymlinks(dir)
		if err != nil || !trustedPath(canonicalDir, trustedRoots) {
			continue
		}
		program := executable(filepath.Join(canonicalDir, "doveadm"), trustedRoots)
		if program == "" {
			continue
		}
		versionProgram := executable(filepath.Join(canonicalDir, "dovecot"), trustedRoots)
		if versionProgram == "" {
			for _, path := range []string{"/usr/sbin/dovecot", "/usr/local/sbin/dovecot"} {
				if versionProgram = executable(path, trustedRoots); versionProgram != "" {
					break
				}
			}
		}
		if versionProgram == "" {
			continue
		}
		out, err := exec.CommandContext(ctx, versionProgram, "--version").Output()
		if err != nil {
			continue
		}
		version := strings.TrimSpace(string(out))
		if !supportedVersion(version) {
			continue
		}
		return Client{Program: program, Version: version}, nil
	}
	return Client{}, errors.New("Dovecot 2.4 or newer client tools are not installed")
}

func StageBackup(ctx context.Context, parent string, target coredb.Target, password string, options BackupOptions, client Client) (_ *StagedBackup, err error) {
	if err := validateTarget(target, password, client); err != nil {
		return nil, err
	}
	if err := validateUsername(options.Username); err != nil {
		return nil, err
	}
	if err := validateMailbox(options.Mailbox); err != nil {
		return nil, err
	}

	root, err := os.MkdirTemp(parent, ".pbs-plus-dovecot-")
	if err != nil {
		return nil, fmt.Errorf("create Dovecot staging directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
		}
	}()
	if err = os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure Dovecot staging directory: %w", err)
	}

	archiveDir := filepath.Join(root, "archive")
	mailDir := filepath.Join(archiveDir, mailDirName)
	secretsDir := filepath.Join(root, "secrets")
	for _, dir := range []string{archiveDir, mailDir, secretsDir} {
		if err = os.Mkdir(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create Dovecot staging subdirectory: %w", err)
		}
	}
	configPath, err := writeClientConfig(secretsDir, mailDir, target.DatabaseCACertificate, password)
	if err != nil {
		return nil, err
	}
	args := []string{"-c", configPath, "backup", "--no-userdb-lookup"}
	if options.Mailbox != "" {
		args = append(args, "-m", options.Mailbox)
	}
	args = append(args, "-R", destination(target))
	if err = run(ctx, client.Program, args, options.Username, options.LogWriter); err != nil {
		return nil, fmt.Errorf("pull Dovecot mailbox: %w", err)
	}
	if err = os.RemoveAll(secretsDir); err != nil {
		return nil, fmt.Errorf("remove Dovecot backup secrets: %w", err)
	}

	manifest := Manifest{
		Version:       ManifestVersion,
		Username:      options.Username,
		Mailbox:       options.Mailbox,
		ClientVersion: client.Version,
		CreatedAt:     time.Now().UTC(),
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode Dovecot manifest: %w", err)
	}
	if err = os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		return nil, fmt.Errorf("write Dovecot manifest: %w", err)
	}
	return &StagedBackup{ArchiveDir: archiveDir, Manifest: manifest, root: root}, nil
}

func RestoreBackup(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, client Client) error {
	if err := validateTarget(target, password, client); err != nil {
		return err
	}
	manifest, err := LoadManifest(archiveDir)
	if err != nil {
		return err
	}
	if err := validateUsername(options.SourceUsername); err != nil {
		return fmt.Errorf("source %w", err)
	}
	if options.SourceUsername != manifest.Username {
		return errors.New("Dovecot source username does not match the backup manifest")
	}
	if options.DestinationUsername == "" {
		options.DestinationUsername = options.SourceUsername
	}
	if err := validateUsername(options.DestinationUsername); err != nil {
		return fmt.Errorf("destination %w", err)
	}
	if err := validateMailbox(options.Mailbox); err != nil {
		return err
	}
	if manifest.Mailbox != "" {
		if options.Mailbox != "" && options.Mailbox != manifest.Mailbox {
			return errors.New("Dovecot mailbox does not match the backup manifest")
		}
		options.Mailbox = manifest.Mailbox
	}

	mailDir := filepath.Join(archiveDir, mailDirName)
	info, err := os.Lstat(mailDir)
	if err != nil {
		return fmt.Errorf("read Dovecot backup mail directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("Dovecot backup mail path is not a directory")
	}
	secretsDir, err := os.MkdirTemp("", ".pbs-plus-dovecot-secrets-")
	if err != nil {
		return fmt.Errorf("create Dovecot restore secrets directory: %w", err)
	}
	defer os.RemoveAll(secretsDir)
	if err := os.Chmod(secretsDir, 0o700); err != nil {
		return fmt.Errorf("secure Dovecot restore secrets directory: %w", err)
	}
	configPath, err := writeClientConfig(secretsDir, mailDir, target.DatabaseCACertificate, password)
	if err != nil {
		return err
	}
	command := "sync"
	args := []string{"-c", configPath, command, "--no-userdb-lookup", "-1"}
	if options.ReplaceExisting {
		command = "backup"
		args = []string{"-c", configPath, command, "--no-userdb-lookup"}
	}
	if options.Mailbox != "" {
		args = append(args, "-m", options.Mailbox)
	}
	args = append(args, destination(target))
	if err := run(ctx, client.Program, args, options.DestinationUsername, options.LogWriter); err != nil {
		return fmt.Errorf("restore Dovecot mailbox: %w", err)
	}
	return nil
}

func LoadManifest(archiveDir string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(archiveDir, manifestName))
	if err != nil {
		return Manifest{}, fmt.Errorf("read Dovecot manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode Dovecot manifest: %w", err)
	}
	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("unsupported Dovecot manifest version %d", manifest.Version)
	}
	if err := validateUsername(manifest.Username); err != nil {
		return Manifest{}, fmt.Errorf("manifest %w", err)
	}
	if err := validateMailbox(manifest.Mailbox); err != nil {
		return Manifest{}, fmt.Errorf("manifest %w", err)
	}
	return manifest, nil
}

func writeClientConfig(dir, mailDir, caPath, password string) (string, error) {
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return "", fmt.Errorf("read Dovecot CA certificate: %w", err)
	}
	localCA := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(localCA, ca, 0o600); err != nil {
		return "", fmt.Errorf("write Dovecot CA certificate: %w", err)
	}
	quotedPassword, err := quoteSetting(password)
	if err != nil {
		return "", err
	}
	config := strings.Join([]string{
		"dovecot_config_version = 2.4.0",
		"dovecot_storage_version = 2.4.0",
		"doveadm_password = " + quotedPassword,
		"ssl_client_ca_file = " + localCA,
		"ssl_client_require_valid_cert = yes",
		"mail_driver = maildir",
		"mail_path = " + mailDir,
		"mail_uid = " + strconv.Itoa(os.Getuid()),
		"mail_gid = " + strconv.Itoa(os.Getgid()),
		"",
	}, "\n")
	configPath := filepath.Join(dir, "dovecot.conf")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return "", fmt.Errorf("write Dovecot client configuration: %w", err)
	}
	return configPath, nil
}

func quoteSetting(value string) (string, error) {
	if value == "" {
		return "", errors.New("Dovecot password is required")
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("Dovecot password contains unsupported control characters")
	}
	value = strings.NewReplacer("\\", "\\\\", "\"", "\\\"").Replace(value)
	return "\"" + value + "\"", nil
}

func run(ctx context.Context, program string, args []string, username string, logWriter io.Writer) error {
	if logWriter == nil {
		logWriter = io.Discard
	}
	cmd := exec.CommandContext(ctx, program, args...)
	cmd.Env = userEnv(username)
	cmd.Stdout = logWriter
	cmd.Stderr = logWriter
	return cmd.Run()
}

func userEnv(username string) []string {
	env := os.Environ()
	for i, e := range slices.Backward(env) {
		if strings.HasPrefix(e, "USER=") {
			env = append(env[:i], env[i+1:]...)
		}
	}
	return append(env, "USER="+username)
}

func validateTarget(target coredb.Target, password string, client Client) error {
	if !target.IsDovecot() {
		return errors.New("target is not a Dovecot target")
	}
	if target.DatabaseHost == "" || target.DatabasePort < 1 || target.DatabasePort > 65535 {
		return errors.New("Dovecot target address is invalid")
	}
	if target.DatabaseCACertificate == "" {
		return errors.New("Dovecot CA certificate is required")
	}
	if _, err := quoteSetting(password); err != nil {
		return err
	}
	if client.Program == "" || !supportedVersion(client.Version) {
		return errors.New("Dovecot 2.4 or newer client is required")
	}
	return nil
}

func validateUsername(username string) error {
	if username == "" {
		return errors.New("Dovecot username is required")
	}
	if strings.ContainsAny(username, "\r\n\x00") {
		return errors.New("Dovecot username contains unsupported control characters")
	}
	return nil
}

func validateMailbox(mailbox string) error {
	if strings.ContainsAny(mailbox, "\r\n\x00") {
		return errors.New("Dovecot mailbox contains unsupported control characters")
	}
	return nil
}

func destination(target coredb.Target) string {
	return "tcps:" + net.JoinHostPort(target.DatabaseHost, strconv.Itoa(target.DatabasePort))
}

func supportedVersion(version string) bool {
	fields := strings.Fields(version)
	if len(fields) == 0 {
		return false
	}
	parts := strings.SplitN(fields[0], ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, majorErr := strconv.Atoi(parts[0])
	minor, minorErr := strconv.Atoi(parts[1])
	return majorErr == nil && minorErr == nil && (major > 2 || major == 2 && minor >= 4)
}

func trustedPath(path string, roots []string) bool {
	for _, root := range roots {
		canonicalRoot, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(canonicalRoot, path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

func executable(path string, trustedRoots []string) string {
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil || !trustedPath(canonical, trustedRoots) {
		return ""
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return ""
	}
	return canonical
}
