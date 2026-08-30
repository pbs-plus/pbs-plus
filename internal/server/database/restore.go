//go:build linux

package database

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

type RestoreOptions struct {
	SourceDatabase      string
	DestinationDatabase string
	ReplaceExisting     bool
}

func (o RestoreOptions) names() (source, destination string) {
	source, destination = o.SourceDatabase, o.DestinationDatabase
	if source == "" {
		source = destination
	}
	if destination == "" {
		destination = source
	}
	return source, destination
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
	source, destination := options.names()
	if manifest.Scope == "database" && destination == "" {
		return errors.New("destination database is required")
	}
	if manifest.Scope == "server" && source == "" && !options.ReplaceExisting {
		return errors.New("whole-server restore requires explicit replacement confirmation")
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
	run := func(stdin io.Reader, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Env = env
		cmd.Stdin = stdin
		return runRestoreCommand(cmd, password)
	}

	source, database := options.names()
	if database != "" {
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

	dump, wait, err := openDumpStream(dumpPath, scope, EnginePostgreSQL, source)
	if err != nil {
		return err
	}
	connectDatabase := "template1"
	if database != "" {
		connectDatabase = database
	}
	_, runErr := run(dump, "--dbname="+connectDatabase)
	dump.Close()
	if err := wait(); err != nil {
		return err
	}
	if runErr != nil {
		return fmt.Errorf("restore PostgreSQL dump: %w", runErr)
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
	run := func(stdin io.Reader, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Stdin = stdin
		return runRestoreCommand(cmd, password)
	}

	source, database := options.names()
	if database != "" {
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

	dump, wait, err := openDumpStream(dumpPath, scope, EngineMySQL, source)
	if err != nil {
		return err
	}
	args := []string(nil)
	if database != "" {
		args = append(args, "--database="+database)
	}
	_, runErr := run(dump, args...)
	dump.Close()
	if err := wait(); err != nil {
		return err
	}
	if runErr != nil {
		return fmt.Errorf("restore MySQL dump: %w", runErr)
	}
	return nil
}

// openDumpStream extracts one database from a server dump, else streams the file as-is; call wait after closing the reader.
// ponytail: skips the dump's globals section, so a single-database restore assumes its roles already exist.
func openDumpStream(dumpPath, scope, engine, database string) (io.ReadCloser, func() error, error) {
	file, err := os.Open(dumpPath)
	if err != nil {
		return nil, nil, fmt.Errorf("open database dump: %w", err)
	}
	if scope != "server" || database == "" {
		return file, func() error { return nil }, nil
	}
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() {
		defer file.Close()
		err := copyDumpSection(writer, bufio.NewReaderSize(file, 1<<20), engine, database)
		writer.CloseWithError(err)
		done <- err
	}()
	return reader, func() error { return <-done }, nil
}

func copyDumpSection(w io.Writer, reader *bufio.Reader, engine, database string) error {
	inSection, found := false, false
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if name, ok := dumpSectionName(engine, line); ok {
				inSection = name == database
				found = found || inSection
			} else if inSection && !skipSectionLine(engine, line) {
				if _, err := w.Write(line); err != nil {
					return nil
				}
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return fmt.Errorf("read database dump: %w", readErr)
		}
	}
	if !found {
		return fmt.Errorf("database %q is not present in the dump", database)
	}
	return nil
}

func dumpSectionName(engine string, line []byte) (string, bool) {
	text := strings.TrimRight(string(line), "\r\n")
	if engine == EnginePostgreSQL {
		rest, ok := strings.CutPrefix(text, `\connect `)
		if !ok {
			return "", false
		}
		return unquoteIdentifier(strings.TrimSpace(rest), '"'), true
	}
	rest, ok := strings.CutPrefix(text, "-- Current Database: ")
	if !ok {
		return "", false
	}
	return unquoteIdentifier(strings.TrimSpace(rest), '`'), true
}

func skipSectionLine(engine string, line []byte) bool {
	if engine != EngineMySQL {
		return false
	}
	text := strings.TrimSpace(string(line))
	return strings.HasPrefix(text, "CREATE DATABASE ") || strings.HasPrefix(text, "USE ")
}

func unquoteIdentifier(value string, quote byte) string {
	if len(value) < 2 || value[0] != quote || value[len(value)-1] != quote {
		return value
	}
	doubled := string([]byte{quote, quote})
	return strings.ReplaceAll(value[1:len(value)-1], doubled, string(quote))
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
