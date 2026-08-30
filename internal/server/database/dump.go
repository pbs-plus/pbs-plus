//go:build linux

package database

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	ManifestVersionV1 = 1
	ManifestVersionV2 = 2
	manifestName      = "manifest.json"
	dumpName          = "dump.sql"
	databasesDirName  = "databases"
)

type DumpOptions struct {
	Scope    string
	Database string
}

type Manifest struct {
	Version       int            `json:"version"`
	Engine        string         `json:"engine"`
	Scope         string         `json:"scope"`
	Database      string         `json:"database,omitempty"`
	DumpFile      string         `json:"dump_file,omitempty"`
	DumpSHA256    string         `json:"dump_sha256,omitempty"`
	Databases     []ManifestFile `json:"databases,omitempty"`
	Globals       []ManifestFile `json:"globals,omitempty"`
	Failed        []string       `json:"failed,omitempty"`
	ClientFamily  string         `json:"client_family"`
	ClientVersion string         `json:"client_version"`
	CreatedAt     time.Time      `json:"created_at"`
}

// ManifestFile is one dump file in a v2 archive; File is relative to the archive directory.
type ManifestFile struct {
	Name   string `json:"name"`
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
}

type StagedDump struct {
	ArchiveDir string
	Manifest   Manifest
	root       string
}

func (s *StagedDump) Cleanup() error {
	if s == nil || s.root == "" {
		return nil
	}
	err := os.RemoveAll(s.root)
	s.root = ""
	return err
}

func StageDump(ctx context.Context, parent string, target coredb.Target, password string, options DumpOptions, bundle ClientBundle) (_ *StagedDump, err error) {
	if err := validateDumpRequest(target, options, bundle); err != nil {
		return nil, err
	}

	root, err := os.MkdirTemp(parent, ".pbs-plus-database-")
	if err != nil {
		return nil, fmt.Errorf("create database staging directory: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(root)
		}
	}()
	if err = os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure database staging directory: %w", err)
	}

	archiveDir := filepath.Join(root, "archive")
	secretsDir := filepath.Join(root, "secrets")
	for _, dir := range []string{archiveDir, secretsDir} {
		if err = os.Mkdir(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create database staging subdirectory: %w", err)
		}
	}

	var manifest Manifest
	if options.Scope == "server" {
		manifest, err = stageServerDump(ctx, archiveDir, target, password, bundle, secretsDir)
	} else {
		manifest, err = stageDatabaseDump(ctx, archiveDir, target, password, options, bundle, secretsDir)
	}
	if err != nil {
		return nil, err
	}
	manifest.ClientFamily = bundle.Family
	manifest.ClientVersion = bundle.Version
	manifest.CreatedAt = time.Now().UTC()
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode database dump manifest: %w", err)
	}
	if err = os.WriteFile(filepath.Join(archiveDir, manifestName), manifestData, 0o600); err != nil {
		return nil, fmt.Errorf("write database dump manifest: %w", err)
	}

	return &StagedDump{ArchiveDir: archiveDir, Manifest: manifest, root: root}, nil
}

// stageDatabaseDump keeps the single-file v1 layout for database scope.
func stageDatabaseDump(ctx context.Context, archiveDir string, target coredb.Target, password string, options DumpOptions, bundle ClientBundle, secretsDir string) (Manifest, error) {
	cmd, err := dumpCommand(ctx, target, password, options, bundle, secretsDir)
	if err != nil {
		return Manifest{}, err
	}
	dumpPath := filepath.Join(archiveDir, dumpName)
	if err := runDumpToFile(cmd, dumpPath, password); err != nil {
		return Manifest{}, err
	}
	digest, err := fileSHA256(dumpPath)
	if err != nil {
		return Manifest{}, err
	}
	return Manifest{
		Version:    ManifestVersionV1,
		Engine:     string(target.Type),
		Scope:      options.Scope,
		Database:   options.Database,
		DumpFile:   dumpName,
		DumpSHA256: digest,
	}, nil
}

// stageServerDump writes one dump file per database plus engine companions, skipping and recording databases that fail.
func stageServerDump(ctx context.Context, archiveDir string, target coredb.Target, password string, bundle ClientBundle, secretsDir string) (Manifest, error) {
	build := func(database string) (*exec.Cmd, error) {
		return dumpCommand(ctx, target, password, DumpOptions{Scope: "database", Database: database}, bundle, secretsDir)
	}
	if target.Type != coredb.TargetTypePostgreSQL {
		databases, err := listMySQLDatabases(ctx, target, password, bundle, secretsDir)
		if err != nil {
			return Manifest{}, err
		}
		grants, err := dumpMySQLGrants(ctx, archiveDir, target, password, bundle, secretsDir)
		if err != nil {
			return Manifest{}, err
		}
		manifest := Manifest{Version: ManifestVersionV2, Engine: string(target.Type), Scope: "server", Globals: []ManifestFile{grants}}
		manifest.Databases, manifest.Failed, err = dumpDatabases(ctx, archiveDir, databases, password, build)
		if err != nil {
			return Manifest{}, err
		}
		return manifest, nil
	}

	databases, err := listPostgreSQLDatabases(ctx, target, password, bundle, secretsDir)
	if err != nil {
		return Manifest{}, err
	}
	globals, err := dumpPostgreSQLGlobals(ctx, archiveDir, target, password, bundle, secretsDir)
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{Version: ManifestVersionV2, Engine: string(target.Type), Scope: "server", Globals: globals}
	manifest.Databases, manifest.Failed, err = dumpDatabases(ctx, archiveDir, databases, password, build)
	if err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func LoadManifest(archiveDir string) (Manifest, error) {
	data, err := os.ReadFile(filepath.Join(archiveDir, manifestName))
	if err != nil {
		return Manifest{}, fmt.Errorf("read database dump manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode database dump manifest: %w", err)
	}
	switch manifest.Version {
	case ManifestVersionV1:
		if err := validateSingleFileManifest(archiveDir, &manifest); err != nil {
			return Manifest{}, err
		}
	case ManifestVersionV2:
		if err := validatePerDatabaseManifest(archiveDir, &manifest); err != nil {
			return Manifest{}, err
		}
	default:
		return Manifest{}, fmt.Errorf("unsupported database dump manifest version %d", manifest.Version)
	}
	return manifest, nil
}

func validateSingleFileManifest(archiveDir string, manifest *Manifest) error {
	if manifest.Engine != EnginePostgreSQL && manifest.Engine != EngineMySQL {
		return fmt.Errorf("unsupported database dump engine %q", manifest.Engine)
	}
	if manifest.Scope != "database" && manifest.Scope != "server" {
		return fmt.Errorf("unsupported database dump scope %q", manifest.Scope)
	}
	if manifest.Scope == "database" && manifest.Database == "" {
		return errors.New("database dump manifest is missing database name")
	}
	if manifest.DumpFile != dumpName {
		return errors.New("database dump manifest has invalid dump file")
	}
	return verifyDumpChecksum(filepath.Join(archiveDir, dumpName), manifest.DumpSHA256)
}

// validatePerDatabaseManifest rejects files escaping the archive directory or failing checksum verification.
func validatePerDatabaseManifest(archiveDir string, manifest *Manifest) error {
	if manifest.Engine != EnginePostgreSQL && manifest.Engine != EngineMySQL {
		return fmt.Errorf("unsupported database dump engine %q", manifest.Engine)
	}
	if manifest.Scope != "server" {
		return errors.New("per-database dump manifests require server scope")
	}
	if manifest.Database != "" || manifest.DumpFile != "" || manifest.DumpSHA256 != "" {
		return errors.New("per-database dump manifest mixes incompatible layouts")
	}
	if len(manifest.Databases) == 0 && len(manifest.Globals) == 0 && len(manifest.Failed) == 0 {
		return errors.New("per-database dump manifest lists no files")
	}
	for _, group := range [][]ManifestFile{manifest.Globals, manifest.Databases} {
		for _, entry := range group {
			path, err := manifestFilePath(archiveDir, entry.File)
			if err != nil {
				return err
			}
			if err := verifyDumpChecksum(path, entry.SHA256); err != nil {
				return fmt.Errorf("database dump %q: %w", entry.Name, err)
			}
		}
	}
	return nil
}

// manifestFilePath resolves a manifest file path, rejecting escapes from the archive directory.
func manifestFilePath(archiveDir, file string) (string, error) {
	if file == "" || filepath.IsAbs(file) || strings.Contains(file, "\x00") {
		return "", errors.New("database dump manifest has invalid file path")
	}
	path := filepath.Join(archiveDir, filepath.FromSlash(file))
	if !strings.HasPrefix(path, filepath.Clean(archiveDir)+string(filepath.Separator)) {
		return "", errors.New("database dump manifest has invalid file path")
	}
	return path, nil
}

func verifyDumpChecksum(path, digest string) error {
	if _, err := hex.DecodeString(digest); err != nil || len(digest) != sha256.Size*2 {
		return errors.New("database dump manifest has invalid checksum")
	}
	actual, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actual, digest) {
		return errors.New("database dump checksum mismatch")
	}
	return nil
}

func validateDumpRequest(target coredb.Target, options DumpOptions, bundle ClientBundle) error {
	if !target.IsDatabase() {
		return errors.New("target is not a database target")
	}
	if bundle.Engine != string(target.Type) {
		return errors.New("database client bundle does not match target engine")
	}
	if options.Scope != "database" && options.Scope != "server" {
		return errors.New("database dump scope must be database or server")
	}
	if options.Scope == "database" && options.Database == "" {
		return errors.New("database name is required for database dump scope")
	}
	if options.Scope == "server" && options.Database != "" {
		return errors.New("database name must be empty for server dump scope")
	}
	if bundle.DumpProgram == "" || bundle.RestoreProgram == "" {
		return errors.New("database client bundle is incomplete")
	}
	if options.Scope == "server" && bundle.ServerDumpProgram == "" {
		return errors.New("database client bundle cannot dump a whole server")
	}
	return nil
}

func dumpCommand(ctx context.Context, target coredb.Target, password string, options DumpOptions, bundle ClientBundle, secretsDir string) (*exec.Cmd, error) {
	if target.Type == coredb.TargetTypePostgreSQL {
		return postgreSQLDumpCommand(ctx, target, password, options, bundle, secretsDir)
	}
	return mySQLDumpCommand(ctx, target, password, options, bundle, secretsDir)
}

func postgreSQLBaseArgs(target coredb.Target) []string {
	return []string{"--host", target.DatabaseHost, "--port", strconv.Itoa(target.DatabasePort), "--username", target.DatabaseUsername, "--no-password"}
}

func mySQLBaseArgs(target coredb.Target, defaultsFile string) []string {
	return []string{
		"--defaults-extra-file=" + defaultsFile,
		"--host=" + target.DatabaseHost,
		"--port=" + strconv.Itoa(target.DatabasePort),
		"--user=" + target.DatabaseUsername,
	}
}

func postgreSQLDumpCommand(ctx context.Context, target coredb.Target, password string, options DumpOptions, bundle ClientBundle, secretsDir string) (*exec.Cmd, error) {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return nil, err
	}

	args := append(postgreSQLBaseArgs(target), "--format=p", options.Database)
	cmd := exec.CommandContext(ctx, bundle.DumpProgram, args...)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+passfile, "PGSSLMODE="+target.DatabaseTLSMode)
	if target.DatabaseCACertificate != "" {
		cmd.Env = append(cmd.Env, "PGSSLROOTCERT="+target.DatabaseCACertificate)
	}
	return cmd, nil
}

func mySQLDumpCommand(ctx context.Context, target coredb.Target, password string, options DumpOptions, bundle ClientBundle, secretsDir string) (*exec.Cmd, error) {
	defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
	if err != nil {
		return nil, err
	}

	args := append(mySQLBaseArgs(target, defaultsFile), "--single-transaction", "--routines", "--events", "--triggers", "--hex-blob")
	args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	args = append(args, options.Database)
	return exec.CommandContext(ctx, bundle.DumpProgram, args...), nil
}

// runDumpToFile captures a dump in a fresh 0600 file, redacting the password from failures.
func runDumpToFile(cmd *exec.Cmd, path, password string) error {
	dump, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create database dump: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stdout = dump
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	closeErr := dump.Close()
	if runErr != nil {
		_ = os.Remove(path)
		message := stderr.String()
		if password != "" {
			message = strings.ReplaceAll(message, password, "[redacted]")
		}
		return fmt.Errorf("database dump failed: %w: %s", runErr, limitedText(message, 4096))
	}
	if closeErr != nil {
		return fmt.Errorf("close database dump: %w", closeErr)
	}
	return nil
}

// dumpDatabases isolates per-database failures into the failed list instead of failing the backup.
func dumpDatabases(ctx context.Context, archiveDir string, databases []string, password string, build func(database string) (*exec.Cmd, error)) (entries []ManifestFile, failed []string, err error) {
	if err := os.Mkdir(filepath.Join(archiveDir, databasesDirName), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create database dump directory: %w", err)
	}
	for index, database := range databases {
		cmd, err := build(database)
		if err != nil {
			return nil, nil, err
		}
		file := fmt.Sprintf("%s/%04d-%s.sql", databasesDirName, index+1, dumpFileLabel(database))
		path := filepath.Join(archiveDir, file)
		if err := runDumpToFile(cmd, path, password); err != nil {
			failed = append(failed, database)
			continue
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, ManifestFile{Name: database, File: file, SHA256: digest})
	}
	if len(entries) == 0 && len(databases) > 0 {
		return nil, nil, fmt.Errorf("all %d databases failed to dump", len(databases))
	}
	return entries, failed, nil
}

// dumpFileLabel keeps filenames safe while the manifest records the real database name.
func dumpFileLabel(name string) string {
	var label strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			label.WriteRune(r)
		default:
			label.WriteByte('_')
		}
	}
	text := label.String()
	if len(text) > 120 {
		text = text[:120]
	}
	if text == "" || text == "." || text == ".." {
		return "database"
	}
	return text
}

func dumpPostgreSQLGlobals(ctx context.Context, archiveDir string, target coredb.Target, password string, bundle ClientBundle, secretsDir string) ([]ManifestFile, error) {
	globals := make([]ManifestFile, 0, 2)
	for _, companion := range []struct {
		name string
		flag string
		file string
	}{
		{name: "roles", flag: "--roles-only", file: "roles.sql"},
		{name: "globals", flag: "--globals-only", file: "globals.sql"},
	} {
		cmd, err := postgreSQLGlobalsDumpCommand(ctx, target, password, bundle, secretsDir, companion.flag)
		if err != nil {
			return nil, err
		}
		path := filepath.Join(archiveDir, companion.file)
		if err := runDumpToFile(cmd, path, password); err != nil {
			return nil, fmt.Errorf("dump PostgreSQL %s: %w", companion.name, err)
		}
		digest, err := fileSHA256(path)
		if err != nil {
			return nil, err
		}
		globals = append(globals, ManifestFile{Name: companion.name, File: companion.file, SHA256: digest})
	}
	return globals, nil
}

func postgreSQLGlobalsDumpCommand(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, secretsDir, flag string) (*exec.Cmd, error) {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return nil, err
	}
	args := append(postgreSQLBaseArgs(target), flag, "--lock-wait-timeout=30s")
	cmd := exec.CommandContext(ctx, bundle.ServerDumpProgram, args...)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+passfile, "PGSSLMODE="+target.DatabaseTLSMode)
	if target.DatabaseCACertificate != "" {
		cmd.Env = append(cmd.Env, "PGSSLROOTCERT="+target.DatabaseCACertificate)
	}
	return cmd, nil
}

// mySQLGrantTableCandidates carry users and privileges across MySQL and MariaDB versions.
var mySQLGrantTableCandidates = []string{
	"user", "db", "tables_priv", "columns_priv", "procs_priv", "proxies_priv",
	"global_priv", "role_edges", "default_roles", "roles_mapping",
}

func dumpMySQLGrants(ctx context.Context, archiveDir string, target coredb.Target, password string, bundle ClientBundle, secretsDir string) (ManifestFile, error) {
	defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
	if err != nil {
		return ManifestFile{}, err
	}
	tables, err := listMySQLGrantTables(ctx, target, password, bundle, defaultsFile)
	if err != nil {
		return ManifestFile{}, err
	}
	args := append(mySQLBaseArgs(target, defaultsFile), "--single-transaction", "--hex-blob")
	args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	args = append(args, "mysql")
	args = append(args, tables...)
	cmd := exec.CommandContext(ctx, bundle.DumpProgram, args...)
	path := filepath.Join(archiveDir, "grants.sql")
	if err := runDumpToFile(cmd, path, password); err != nil {
		return ManifestFile{}, fmt.Errorf("dump MySQL grants: %w", err)
	}
	digest, err := fileSHA256(path)
	if err != nil {
		return ManifestFile{}, err
	}
	return ManifestFile{Name: "grants", File: "grants.sql", SHA256: digest}, nil
}

func listMySQLGrantTables(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, defaultsFile string) ([]string, error) {
	args := append(mySQLBaseArgs(target, defaultsFile), "--batch", "--skip-column-names", "--execute=SHOW TABLES FROM mysql")
	args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	names, err := readDatabaseNames(exec.CommandContext(ctx, bundle.RestoreProgram, args...), password)
	if err != nil {
		return nil, fmt.Errorf("list MySQL grant tables: %w", err)
	}
	available := make(map[string]struct{}, len(names))
	for _, name := range names {
		available[name] = struct{}{}
	}
	tables := make([]string, 0, len(mySQLGrantTableCandidates))
	for _, table := range mySQLGrantTableCandidates {
		if _, ok := available[table]; ok {
			tables = append(tables, table)
		}
	}
	if len(tables) == 0 {
		return nil, errors.New("no MySQL grant tables found")
	}
	return tables, nil
}

func listPostgreSQLDatabases(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, secretsDir string) ([]string, error) {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return nil, err
	}
	args := append(postgreSQLBaseArgs(target), "--dbname=template1", "--tuples-only", "--no-align",
		"--command=SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate")
	cmd := exec.CommandContext(ctx, bundle.RestoreProgram, args...)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+passfile, "PGSSLMODE="+target.DatabaseTLSMode)
	if target.DatabaseCACertificate != "" {
		cmd.Env = append(cmd.Env, "PGSSLROOTCERT="+target.DatabaseCACertificate)
	}
	return readDatabaseNames(cmd, password)
}

// listMySQLDatabases lists user databases, excluding system schemas covered by the grants companion.
func listMySQLDatabases(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, secretsDir string) ([]string, error) {
	defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
	if err != nil {
		return nil, err
	}
	args := append(mySQLBaseArgs(target, defaultsFile), "--batch", "--skip-column-names", "--execute=SHOW DATABASES")
	args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	cmd := exec.CommandContext(ctx, bundle.RestoreProgram, args...)
	names, err := readDatabaseNames(cmd, password)
	if err != nil {
		return nil, err
	}
	databases := make([]string, 0, len(names))
	for _, name := range names {
		if _, system := mySQLSystemDatabases[name]; !system {
			databases = append(databases, name)
		}
	}
	return databases, nil
}

var mySQLSystemDatabases = map[string]struct{}{
	"information_schema": {},
	"performance_schema": {},
	"mysql":              {},
	"sys":                {},
}

func readDatabaseNames(cmd *exec.Cmd, password string) ([]string, error) {
	out, err := runClientCommand(cmd, password)
	if err != nil {
		return nil, fmt.Errorf("list databases: %w", err)
	}
	names := make([]string, 0, 8)
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if name := strings.TrimSpace(line); name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

func mySQLTLSArgs(mode, caCertificate, family string) []string {
	if family == FamilyMySQL {
		args := []string{"--ssl-mode=" + strings.ToUpper(mode)}
		if caCertificate != "" {
			args = append(args, "--ssl-ca="+caCertificate)
		}
		return args
	}

	args := make([]string, 0, 3)
	switch mode {
	case "disabled":
		return []string{"--skip-ssl"}
	case "verify-identity":
		args = append(args, "--ssl", "--ssl-verify-server-cert")
	default:
		args = append(args, "--ssl")
	}
	if caCertificate != "" {
		args = append(args, "--ssl-ca="+caCertificate)
	}
	return args
}

func pgpassEscape(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	return strings.ReplaceAll(value, ":", "\\:")
}

func writePostgreSQLPassfile(dir string, target coredb.Target, password string) (string, error) {
	for _, value := range []string{target.DatabaseHost, target.DatabaseUsername, password} {
		if strings.ContainsAny(value, "\r\n") {
			return "", errors.New("PostgreSQL credentials cannot contain line breaks")
		}
	}
	path := filepath.Join(dir, "pgpass")
	line := strings.Join([]string{
		pgpassEscape(target.DatabaseHost),
		strconv.Itoa(target.DatabasePort),
		"*",
		pgpassEscape(target.DatabaseUsername),
		pgpassEscape(password),
	}, ":") + "\n"
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		return "", fmt.Errorf("write PostgreSQL password file: %w", err)
	}
	return path, nil
}

func writeMySQLDefaultsFile(dir, password string) (string, error) {
	path := filepath.Join(dir, "client.cnf")
	contents := strings.Join([]string{
		"[client]",
		"password=" + strconv.Quote(password),
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		return "", fmt.Errorf("write MySQL password file: %w", err)
	}
	return path, nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open database dump for checksum: %w", err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("checksum database dump: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func limitedText(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
