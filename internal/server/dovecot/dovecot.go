//go:build linux

package dovecot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	ManifestVersion = 2
	manifestName    = "dovecot-manifest.json"
	mailDirName     = "mail"
	sieveDirName    = "sieve"
	usersDirName    = "users"

	ScopeUser   = "user"
	ScopeServer = "server"
)

type Client struct {
	Program string
	Version string
}

type BackupOptions struct {
	Scope     string
	Username  string
	Mailbox   string
	LogWriter io.Writer
}

type RestoreOptions struct {
	SourceUsername      string
	DestinationUsername string
	SourceMailbox       string
	DestinationMailbox  string
	Messages            []string
	ReplaceExisting     bool
	StagingDir          string
	LogWriter           io.Writer
}

// ManifestUser maps one backed-up mailbox user to its archive directory.
// Dir is empty for version 1 archives, which hold a single user at the root.
type ManifestUser struct {
	Username string `json:"username"`
	Dir      string `json:"dir,omitempty"`
}

type Manifest struct {
	Version       int            `json:"version"`
	Scope         string         `json:"scope,omitempty"`
	Host          string         `json:"host,omitempty"`
	Username      string         `json:"username,omitempty"`
	Mailbox       string         `json:"mailbox,omitempty"`
	Users         []ManifestUser `json:"users,omitempty"`
	Failed        []string       `json:"failed,omitempty"`
	ClientVersion string         `json:"client_version"`
	CreatedAt     time.Time      `json:"created_at"`
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
	scope, err := normalizeScope(options.Scope)
	if err != nil {
		return nil, err
	}
	if err := validateMailbox(options.Mailbox); err != nil {
		return nil, err
	}

	usernames := []string{options.Username}
	if scope == ScopeServer {
		if usernames, err = ListUsers(ctx, target, password); err != nil {
			return nil, err
		}
		if len(usernames) == 0 {
			return nil, errors.New("Dovecot target reported no mailbox users")
		}
	}
	for _, username := range usernames {
		if err := validateUsername(username); err != nil {
			return nil, err
		}
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
	secretsDir := filepath.Join(root, "secrets")
	for _, dir := range []string{archiveDir, secretsDir} {
		if err = os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create Dovecot staging subdirectory: %w", err)
		}
	}
	if uid, gid := mailIdentity(); os.Getuid() == 0 && uid != 0 {
		if err = os.Chown(root, uid, gid); err != nil {
			return nil, fmt.Errorf("prepare Dovecot staging permissions: %w", err)
		}
		if err = os.Chown(archiveDir, uid, gid); err != nil {
			return nil, fmt.Errorf("prepare Dovecot staging permissions: %w", err)
		}
	}

	manifest := Manifest{
		Version:       ManifestVersion,
		Scope:         scope,
		Host:          target.DatabaseHost,
		Mailbox:       options.Mailbox,
		ClientVersion: client.Version,
		CreatedAt:     time.Now().UTC(),
	}
	for index, username := range usernames {
		dir := UserDirName(username)
		userDir := filepath.Join(archiveDir, usersDirName, dir)
		mailDir := filepath.Join(userDir, mailDirName)
		userSecretsDir := filepath.Join(secretsDir, strconv.Itoa(index))
		for _, path := range []string{mailDir, filepath.Join(userDir, sieveDirName, "scripts"), userSecretsDir} {
			if err = os.MkdirAll(path, 0o700); err != nil {
				return nil, fmt.Errorf("create Dovecot staging subdirectory: %w", err)
			}
		}
		if err = chownForMail(filepath.Join(archiveDir, usersDirName)); err != nil {
			return nil, fmt.Errorf("prepare Dovecot staging permissions: %w", err)
		}
		var configPath string
		if configPath, err = writeClientConfig(userSecretsDir, mailDir, target.DatabaseCACertificate, password); err != nil {
			return nil, err
		}
		args := []string{"-c", configPath, "backup", "--no-userdb-lookup"}
		if options.Mailbox != "" {
			args = append(args, "-m", options.Mailbox)
		}
		args = append(args, "-R", destination(target))
		if runErr := run(ctx, client.Program, args, username, options.LogWriter); runErr != nil {
			if scope == ScopeUser || ctx.Err() != nil {
				err = fmt.Errorf("pull Dovecot mailbox: %w", runErr)
				return nil, err
			}
			logLine(options.LogWriter, fmt.Sprintf("skipping %s: %s", username, runErr))
			manifest.Failed = append(manifest.Failed, username)
			if err = os.RemoveAll(userDir); err != nil {
				return nil, fmt.Errorf("remove failed Dovecot user staging: %w", err)
			}
			continue
		}
		manifest.Users = append(manifest.Users, ManifestUser{Username: username, Dir: dir})
	}
	if err = os.RemoveAll(secretsDir); err != nil {
		return nil, fmt.Errorf("remove Dovecot backup secrets: %w", err)
	}
	if len(manifest.Users) == 0 {
		err = errors.New("no Dovecot mailbox was backed up")
		return nil, err
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
	if options.SourceUsername != "" {
		if err := validateUsername(options.SourceUsername); err != nil {
			return fmt.Errorf("source %w", err)
		}
	}
	if options.DestinationUsername != "" {
		if err := validateUsername(options.DestinationUsername); err != nil {
			return fmt.Errorf("destination %w", err)
		}
	}
	if err := validateMailbox(options.SourceMailbox); err != nil {
		return err
	}
	if err := validateMailbox(options.DestinationMailbox); err != nil {
		return err
	}
	selective := len(options.Messages) > 0 || options.DestinationMailbox != ""
	if options.ReplaceExisting && selective {
		return errors.New("Dovecot replace-existing cannot select messages or a destination mailbox")
	}
	if manifest.Mailbox != "" {
		if options.SourceMailbox != "" && options.SourceMailbox != manifest.Mailbox {
			return errors.New("Dovecot mailbox does not match the backup manifest")
		}
		options.SourceMailbox = manifest.Mailbox
	}

	users, err := selectManifestUsers(manifest, options.SourceUsername)
	if err != nil {
		return err
	}
	if len(users) > 1 && (options.DestinationUsername != "" || selective) {
		return errors.New("Dovecot destination username, destination mailbox and message selection each require one source username")
	}
	for _, user := range users {
		if err := restoreUser(ctx, archiveDir, user, target, password, options, client); err != nil {
			return err
		}
	}
	return nil
}

func restoreUser(ctx context.Context, archiveDir string, user ManifestUser, target coredb.Target, password string, options RestoreOptions, client Client) error {
	userDir := UserArchiveDir(archiveDir, user)
	if user.Dir != "" {
		if _, err := os.Lstat(userDir); errors.Is(err, fs.ErrNotExist) {
			flattenedDir := filepath.Join(archiveDir, user.Dir)
			if _, err := os.Lstat(flattenedDir); err == nil {
				userDir = flattenedDir
			}
		}
	}
	mailDir := filepath.Join(userDir, mailDirName)
	info, err := os.Lstat(mailDir)
	if err != nil {
		return fmt.Errorf("read Dovecot backup mail directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("Dovecot backup mail path is not a directory")
	}

	mailbox := options.SourceMailbox
	if len(options.Messages) > 0 || options.DestinationMailbox != "" {
		selectionRoot, selectedMailbox, err := buildSelection(options.StagingDir, mailDir, options.SourceMailbox, options.DestinationMailbox, options.Messages)
		if err != nil {
			return err
		}
		defer func() {
			_ = os.RemoveAll(selectionRoot)
		}()
		userDir = selectionRoot
		mailDir = filepath.Join(selectionRoot, mailDirName)
		mailbox = selectedMailbox
	}

	if err := os.MkdirAll(filepath.Join(userDir, sieveDirName, "scripts"), 0o700); err != nil {
		return fmt.Errorf("create Dovecot restore sieve directory: %w", err)
	}
	archiveInfo, err := os.Stat(archiveDir)
	if err != nil {
		return fmt.Errorf("read Dovecot restore staging directory: %w", err)
	}
	if err := os.Chmod(archiveDir, archiveInfo.Mode().Perm()|0o111); err != nil {
		return fmt.Errorf("prepare Dovecot restore staging directory: %w", err)
	}
	if err := chownForMail(userDir); err != nil {
		return fmt.Errorf("prepare Dovecot restore permissions: %w", err)
	}
	secretsDir, err := os.MkdirTemp("", ".pbs-plus-dovecot-secrets-")
	if err != nil {
		return fmt.Errorf("create Dovecot restore secrets directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(secretsDir)
	}()
	if err := os.Chmod(secretsDir, 0o700); err != nil {
		return fmt.Errorf("secure Dovecot restore secrets directory: %w", err)
	}
	configPath, err := writeClientConfig(secretsDir, mailDir, target.DatabaseCACertificate, password)
	if err != nil {
		return err
	}

	destinationUsername := options.DestinationUsername
	if destinationUsername == "" {
		destinationUsername = user.Username
	}
	command := "sync"
	args := []string{"-c", configPath, command, "--no-userdb-lookup", "-1"}
	passes := 2
	if options.ReplaceExisting {
		command = "backup"
		args = []string{"-c", configPath, command, "--no-userdb-lookup"}
		passes = 1
	}
	if mailbox != "" {
		args = append(args, "-m", mailbox)
	}
	args = append(args, destination(target))
	// mailbox GUID state when the destination mailbox already exists. pi:keep
	for pass := 0; pass < passes; pass++ {
		if err := run(ctx, client.Program, args, destinationUsername, options.LogWriter); err != nil {
			return fmt.Errorf("restore Dovecot mailbox: %w", err)
		}
	}
	return nil
}

// selectManifestUsers resolves which backed-up users a restore covers.
func selectManifestUsers(manifest Manifest, username string) ([]ManifestUser, error) {
	if username == "" {
		return manifest.Users, nil
	}
	for _, user := range manifest.Users {
		if user.Username == username {
			return []ManifestUser{user}, nil
		}
	}
	return nil, fmt.Errorf("Dovecot snapshot does not contain user %q", username)
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
	switch manifest.Version {
	case 1:
		if err := validateUsername(manifest.Username); err != nil {
			return Manifest{}, fmt.Errorf("manifest %w", err)
		}
		manifest.Scope = ScopeUser
		manifest.Users = []ManifestUser{{Username: manifest.Username}}
	case ManifestVersion:
		if manifest.Scope == "" {
			manifest.Scope = ScopeUser
		}
		if len(manifest.Users) == 0 {
			return Manifest{}, errors.New("Dovecot manifest contains no users")
		}
		for _, user := range manifest.Users {
			if err := validateUsername(user.Username); err != nil {
				return Manifest{}, fmt.Errorf("manifest %w", err)
			}
			if err := validateUserDir(user.Dir); err != nil {
				return Manifest{}, fmt.Errorf("manifest %w", err)
			}
		}
	default:
		return Manifest{}, fmt.Errorf("unsupported Dovecot manifest version %d", manifest.Version)
	}
	if err := validateMailbox(manifest.Mailbox); err != nil {
		return Manifest{}, fmt.Errorf("manifest %w", err)
	}
	return manifest, nil
}

// UserArchiveDir resolves where one user's mail lives inside an archive.
func UserArchiveDir(archiveDir string, user ManifestUser) string {
	if user.Dir == "" {
		return archiveDir
	}
	return filepath.Join(archiveDir, usersDirName, user.Dir)
}

// ArchiveUserPaths lists the snapshot paths holding one user's backup, so a
// restore pulls that user instead of every mailbox in a server-scope snapshot.
func ArchiveUserPaths(username string) []string {
	return []string{"/" + manifestName, "/" + usersDirName + "/" + UserDirName(username)}
}

func normalizeScope(scope string) (string, error) {
	switch scope {
	case "", ScopeUser:
		return ScopeUser, nil
	case ScopeServer:
		return ScopeServer, nil
	default:
		return "", fmt.Errorf("invalid Dovecot backup scope %q", scope)
	}
}

func logLine(writer io.Writer, line string) {
	if writer == nil {
		return
	}
	if _, err := fmt.Fprintln(writer, line); err != nil {
		log.Error(err, "")
	}
}

// systemCABundlePaths are the host trust stores used when a target has no CA of its own.
var systemCABundlePaths = []string{
	"/etc/ssl/certs/ca-certificates.crt",
	"/etc/pki/tls/certs/ca-bundle.crt",
	"/etc/ssl/cert.pem",
}

// readCACertificate falls back to the host trust store so a target without its
// own CA still verifies the remote certificate.
func readCACertificate(caPath string) ([]byte, error) {
	if caPath == "" {
		for _, candidate := range systemCABundlePaths {
			if _, err := os.Stat(candidate); err == nil {
				caPath = candidate
				break
			}
		}
		if caPath == "" {
			return nil, errors.New("Dovecot target has no CA certificate and no system CA bundle was found")
		}
	}
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read Dovecot CA certificate: %w", err)
	}
	return ca, nil
}

func writeClientConfig(dir, mailDir, caPath, password string) (string, error) {
	ca, err := readCACertificate(caPath)
	if err != nil {
		return "", err
	}
	localCA := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(localCA, ca, 0o600); err != nil {
		return "", fmt.Errorf("write Dovecot CA certificate: %w", err)
	}
	setting, err := passwordSetting(password)
	if err != nil {
		return "", err
	}
	sieveDir := filepath.Join(filepath.Dir(mailDir), "sieve")
	uid, gid := mailIdentity()
	config := strings.Join([]string{
		"dovecot_config_version = 2.4.0",
		"dovecot_storage_version = 2.4.0",
		"doveadm_password = " + setting,
		"ssl_client_ca_file = " + localCA,
		"ssl_client_require_valid_cert = yes",
		"mail_driver = maildir",
		"mail_path = " + mailDir,
		"mail_uid = " + strconv.Itoa(uid),
		"mail_gid = " + strconv.Itoa(gid),
		"first_valid_uid = 0",
		"first_valid_gid = 0",
		"sieve_script personal {",
		"	driver = file",
		"	path = " + filepath.Join(sieveDir, "scripts"),
		"	active_path = " + filepath.Join(sieveDir, "main.sieve"),
		"}",
		"",
	}, "\n")
	configPath := filepath.Join(dir, "dovecot.conf")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return "", fmt.Errorf("write Dovecot client configuration: %w", err)
	}
	return configPath, nil
}

// passwordSetting validates the shared doveadm password and returns it in a
// form safe for the generated client configuration. doveadm compares the
// value literally, so quoting or escaping is not possible: reject values
// that cannot appear verbatim in a Dovecot setting.
func passwordSetting(value string) (string, error) {
	if value == "" {
		return "", errors.New("Dovecot password is required")
	}
	if strings.ContainsFunc(value, func(r rune) bool {
		return r < 0x20 || r > 0x7e || strings.ContainsRune(" \t\"\\#{};", r)
	}) {
		return "", errors.New("Dovecot password must contain only printable characters without spaces, quotes, or braces")
	}
	return value, nil
}

// mailIdentity returns the uid and gid doveadm drops to when accessing
// staged mail. Dovecot refuses mail access for root, so the root-run server
// stages mail as nobody (65534) instead.
func mailIdentity() (int, int) {
	if uid := os.Getuid(); uid != 0 {
		return uid, os.Getgid()
	}
	return 65534, 65534
}

// chownForMail makes staged files writable by the mail identity when the
// server runs with enough privilege to do so.
func chownForMail(path string) error {
	uid, gid := mailIdentity()
	if os.Getuid() != 0 || (uid == 0 && gid == 0) {
		return nil
	}
	return filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chown(p, uid, gid)
	})
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
	if _, err := passwordSetting(password); err != nil {
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
