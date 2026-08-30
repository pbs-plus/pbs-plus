//go:build linux

package database

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type RestoreOptions struct {
	DestinationDatabase string
	ReplaceExisting     bool
}

func RestoreDump(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, bundle ClientBundle) error {
	manifest, err := LoadManifest(archiveDir)
	if err != nil {
		return err
	}
	if !target.IsDatabase() || manifest.Engine != string(target.Type) {
		return errors.New("database dump engine does not match destination target")
	}
	if bundle.Engine != manifest.Engine || bundle.RestoreProgram == "" {
		return errors.New("database restore client does not match dump engine")
	}
	if manifest.Scope == "database" && options.DestinationDatabase == "" {
		return errors.New("destination database is required")
	}
	if manifest.Scope == "server" {
		if options.DestinationDatabase != "" {
			return errors.New("destination database must be empty for a whole-server restore")
		}
		if !options.ReplaceExisting {
			return errors.New("whole-server restore requires explicit replacement confirmation")
		}
	}

	secretsDir, err := os.MkdirTemp("", ".pbs-plus-database-secrets-")
	if err != nil {
		return fmt.Errorf("create database restore secrets directory: %w", err)
	}
	defer os.RemoveAll(secretsDir)
	if err := os.Chmod(secretsDir, 0o700); err != nil {
		return fmt.Errorf("secure database restore secrets directory: %w", err)
	}

	dumpPath := filepath.Join(archiveDir, manifest.DumpFile)
	if target.Type == coredb.TargetTypePostgreSQL {
		return restorePostgreSQL(ctx, dumpPath, target, password, options, manifest.Scope, bundle, secretsDir)
	}
	return restoreMySQL(ctx, dumpPath, target, password, options, manifest.Scope, bundle, secretsDir)
}

func restorePostgreSQL(ctx context.Context, dumpPath string, target coredb.Target, password string, options RestoreOptions, scope string, bundle ClientBundle, secretsDir string) error {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return err
	}
	baseArgs := []string{
		"--host", target.DatabaseHost,
		"--port", strconv.Itoa(target.DatabasePort),
		"--username", target.DatabaseUsername,
		"--no-password",
		"--set=ON_ERROR_STOP=1",
	}
	env := append(os.Environ(), "PGPASSFILE="+passfile, "PGSSLMODE="+target.DatabaseTLSMode)
	if target.DatabaseCACertificate != "" {
		env = append(env, "PGSSLROOTCERT="+target.DatabaseCACertificate)
	}
	run := func(stdin *os.File, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Env = env
		cmd.Stdin = stdin
		return runRestoreCommand(cmd, password)
	}

	database := options.DestinationDatabase
	if scope == "database" {
		query := "SELECT 1 FROM pg_database WHERE datname = " + postgreSQLLiteral(database)
		out, err := run(nil, "--dbname=template1", "--tuples-only", "--no-align", "--command="+query)
		if err != nil {
			return fmt.Errorf("check PostgreSQL destination database: %w", err)
		}
		exists := strings.TrimSpace(string(out)) == "1"
		if exists && !options.ReplaceExisting {
			return fmt.Errorf("destination database %q already exists", database)
		}
		if exists {
			if _, err := run(nil, "--dbname=template1", "--command=DROP DATABASE "+postgreSQLIdentifier(database)); err != nil {
				return fmt.Errorf("drop PostgreSQL destination database: %w", err)
			}
		}
		if _, err := run(nil, "--dbname=template1", "--command=CREATE DATABASE "+postgreSQLIdentifier(database)); err != nil {
			return fmt.Errorf("create PostgreSQL destination database: %w", err)
		}
	}

	dump, err := os.Open(dumpPath)
	if err != nil {
		return fmt.Errorf("open database dump: %w", err)
	}
	defer dump.Close()
	connectDatabase := "template1"
	if scope == "database" {
		connectDatabase = database
	}
	if _, err := run(dump, "--dbname="+connectDatabase); err != nil {
		return fmt.Errorf("restore PostgreSQL dump: %w", err)
	}
	return nil
}

func restoreMySQL(ctx context.Context, dumpPath string, target coredb.Target, password string, options RestoreOptions, scope string, bundle ClientBundle, secretsDir string) error {
	defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
	if err != nil {
		return err
	}
	baseArgs := []string{
		"--defaults-extra-file=" + defaultsFile,
		"--host=" + target.DatabaseHost,
		"--port=" + strconv.Itoa(target.DatabasePort),
		"--user=" + target.DatabaseUsername,
	}
	baseArgs = append(baseArgs, mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	run := func(stdin *os.File, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Stdin = stdin
		return runRestoreCommand(cmd, password)
	}

	database := options.DestinationDatabase
	if scope == "database" {
		encodedName := hex.EncodeToString([]byte(database))
		query := "SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = CONVERT(0x" + encodedName + " USING utf8mb4)"
		out, err := run(nil, "--batch", "--skip-column-names", "--execute="+query)
		if err != nil {
			return fmt.Errorf("check MySQL destination database: %w", err)
		}
		exists := strings.TrimSpace(string(out)) == "1"
		if exists && !options.ReplaceExisting {
			return fmt.Errorf("destination database %q already exists", database)
		}
		if exists {
			if _, err := run(nil, "--execute=DROP DATABASE "+mySQLIdentifier(database)); err != nil {
				return fmt.Errorf("drop MySQL destination database: %w", err)
			}
		}
		if _, err := run(nil, "--execute=CREATE DATABASE "+mySQLIdentifier(database)); err != nil {
			return fmt.Errorf("create MySQL destination database: %w", err)
		}
	}

	dump, err := os.Open(dumpPath)
	if err != nil {
		return fmt.Errorf("open database dump: %w", err)
	}
	defer dump.Close()
	args := []string(nil)
	if scope == "database" {
		args = append(args, "--database="+database)
	}
	if _, err := run(dump, args...); err != nil {
		return fmt.Errorf("restore MySQL dump: %w", err)
	}
	return nil
}

func runRestoreCommand(cmd *exec.Cmd, password string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		message := stderr.String()
		if password != "" {
			message = strings.ReplaceAll(message, password, "[redacted]")
		}
		return nil, fmt.Errorf("%w: %s", err, limitedText(message, 4096))
	}
	return stdout.Bytes(), nil
}

func postgreSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func postgreSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func mySQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
