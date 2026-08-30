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
	ManifestVersion = 1
	manifestName    = "manifest.json"
	dumpName        = "dump.sql"
)

type DumpOptions struct {
	Scope    string
	Database string
}

type Manifest struct {
	Version       int       `json:"version"`
	Engine        string    `json:"engine"`
	Scope         string    `json:"scope"`
	Database      string    `json:"database,omitempty"`
	DumpFile      string    `json:"dump_file"`
	DumpSHA256    string    `json:"dump_sha256"`
	ClientFamily  string    `json:"client_family"`
	ClientVersion string    `json:"client_version"`
	CreatedAt     time.Time `json:"created_at"`
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

	cmd, err := dumpCommand(ctx, target, password, options, bundle, secretsDir)
	if err != nil {
		return nil, err
	}
	dumpPath := filepath.Join(archiveDir, dumpName)
	dump, err := os.OpenFile(dumpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create database dump: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stdout = dump
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	closeErr := dump.Close()
	if runErr != nil {
		message := stderr.String()
		if password != "" {
			message = strings.ReplaceAll(message, password, "[redacted]")
		}
		return nil, fmt.Errorf("database dump failed: %w: %s", runErr, limitedText(message, 4096))
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close database dump: %w", closeErr)
	}

	digest, err := fileSHA256(dumpPath)
	if err != nil {
		return nil, err
	}
	manifest := Manifest{
		Version:       ManifestVersion,
		Engine:        string(target.Type),
		Scope:         options.Scope,
		Database:      options.Database,
		DumpFile:      dumpName,
		DumpSHA256:    digest,
		ClientFamily:  bundle.Family,
		ClientVersion: bundle.Version,
		CreatedAt:     time.Now().UTC(),
	}
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode database dump manifest: %w", err)
	}
	if err = os.WriteFile(filepath.Join(archiveDir, manifestName), manifestData, 0o600); err != nil {
		return nil, fmt.Errorf("write database dump manifest: %w", err)
	}

	return &StagedDump{ArchiveDir: archiveDir, Manifest: manifest, root: root}, nil
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
	if manifest.Version != ManifestVersion {
		return Manifest{}, fmt.Errorf("unsupported database dump manifest version %d", manifest.Version)
	}
	if manifest.Engine != EnginePostgreSQL && manifest.Engine != EngineMySQL {
		return Manifest{}, fmt.Errorf("unsupported database dump engine %q", manifest.Engine)
	}
	if manifest.Scope != "database" && manifest.Scope != "server" {
		return Manifest{}, fmt.Errorf("unsupported database dump scope %q", manifest.Scope)
	}
	if manifest.Scope == "database" && manifest.Database == "" {
		return Manifest{}, errors.New("database dump manifest is missing database name")
	}
	if manifest.DumpFile != dumpName {
		return Manifest{}, errors.New("database dump manifest has invalid dump file")
	}
	if _, err := hex.DecodeString(manifest.DumpSHA256); err != nil || len(manifest.DumpSHA256) != sha256.Size*2 {
		return Manifest{}, errors.New("database dump manifest has invalid checksum")
	}
	digest, err := fileSHA256(filepath.Join(archiveDir, dumpName))
	if err != nil {
		return Manifest{}, err
	}
	if !strings.EqualFold(digest, manifest.DumpSHA256) {
		return Manifest{}, errors.New("database dump checksum mismatch")
	}
	return manifest, nil
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

func postgreSQLDumpCommand(ctx context.Context, target coredb.Target, password string, options DumpOptions, bundle ClientBundle, secretsDir string) (*exec.Cmd, error) {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return nil, err
	}

	program := bundle.DumpProgram
	args := []string{"--host", target.DatabaseHost, "--port", strconv.Itoa(target.DatabasePort), "--username", target.DatabaseUsername, "--no-password"}
	if options.Scope == "database" {
		args = append(args, "--format=p", options.Database)
	} else {
		program = bundle.ServerDumpProgram
		args = append(args, "--lock-wait-timeout=30s")
	}
	cmd := exec.CommandContext(ctx, program, args...)
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

	args := []string{
		"--defaults-extra-file=" + defaultsFile,
		"--host=" + target.DatabaseHost,
		"--port=" + strconv.Itoa(target.DatabasePort),
		"--user=" + target.DatabaseUsername,
		"--single-transaction",
		"--routines",
		"--events",
		"--triggers",
		"--hex-blob",
	}
	args = append(args, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	if options.Scope == "database" {
		args = append(args, options.Database)
	} else {
		args = append(args, "--all-databases")
	}
	return exec.CommandContext(ctx, bundle.DumpProgram, args...), nil
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
