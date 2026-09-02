//go:build linux

package database

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
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
	if target.Type == coredb.TargetTypeLDAP {
		return restoreLDAP(ctx, archiveDir, target, password, options, manifest, bundle)
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

	if manifest.Version >= ManifestVersionV2 {
		return restorePerDatabaseDump(ctx, archiveDir, target, password, options, manifest, bundle, secretsDir)
	}
	dumpPath := filepath.Join(archiveDir, manifest.DumpFile)
	if target.Type == coredb.TargetTypePostgreSQL {
		return restorePostgreSQL(ctx, dumpPath, target, password, options, manifest.Scope, bundle, secretsDir)
	}
	return restoreMySQL(ctx, dumpPath, target, password, options, manifest.Scope, bundle, secretsDir)
}

// restorePerDatabaseDump restores v2 archives: one database from its own
// file, or the whole server by replaying companions then every file.
func restorePerDatabaseDump(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, manifest Manifest, bundle ClientBundle, secretsDir string) error {
	source, destination := options.names()
	if destination != "" {
		entry, err := manifest.findDatabase(source)
		if err != nil {
			return err
		}
		dumpPath, err := manifestFilePath(archiveDir, entry.File)
		if err != nil {
			return err
		}
		if target.Type == coredb.TargetTypePostgreSQL {
			return restorePostgreSQL(ctx, dumpPath, target, password, options, "database", bundle, secretsDir)
		}
		return restoreMySQL(ctx, dumpPath, target, password, options, "database", bundle, secretsDir)
	}
	if target.Type == coredb.TargetTypePostgreSQL {
		return restorePostgreSQLServer(ctx, archiveDir, target, password, options, manifest, bundle, secretsDir)
	}
	return restoreMySQLServer(ctx, archiveDir, target, password, options, manifest, bundle, secretsDir)
}

// findDatabase locates one database's dump file or explains its absence.
func (m Manifest) findDatabase(name string) (ManifestFile, error) {
	for _, entry := range m.Databases {
		if entry.Name == name {
			return entry, nil
		}
	}
	if slices.Contains(m.Failed, name) {
		return ManifestFile{}, fmt.Errorf("database %q failed to dump and is not part of the snapshot", name)
	}
	return ManifestFile{}, fmt.Errorf("database %q is not present in the dump", name)
}

// restoreManifestFiles pipes each listed file through the restore callback in manifest order.
func restoreManifestFiles(archiveDir string, entries []ManifestFile, restore func(io.Reader) error) error {
	for _, entry := range entries {
		path, err := manifestFilePath(archiveDir, entry.File)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open database dump %q: %w", entry.Name, err)
		}
		restoreErr := restore(file)
		closeErr := file.Close()
		if restoreErr != nil {
			return fmt.Errorf("restore %s: %w", entry.Name, restoreErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close database dump %q: %w", entry.Name, closeErr)
		}
	}
	return nil
}

// restoreManifestDatabase restores one database file into a database of the same name.
func restoreManifestDatabase(ctx context.Context, archiveDir string, entry ManifestFile, target coredb.Target, password string, options RestoreOptions, bundle ClientBundle, secretsDir string) error {
	dumpPath, err := manifestFilePath(archiveDir, entry.File)
	if err != nil {
		return err
	}
	databaseOptions := RestoreOptions{SourceDatabase: entry.Name, DestinationDatabase: entry.Name, ReplaceExisting: options.ReplaceExisting}
	if target.Type == coredb.TargetTypePostgreSQL {
		if err := restorePostgreSQL(ctx, dumpPath, target, password, databaseOptions, "database", bundle, secretsDir); err != nil {
			return fmt.Errorf("restore database %q: %w", entry.Name, err)
		}
		return nil
	}
	if err := restoreMySQL(ctx, dumpPath, target, password, databaseOptions, "database", bundle, secretsDir); err != nil {
		return fmt.Errorf("restore database %q: %w", entry.Name, err)
	}
	return nil
}

// restorePostgreSQLServer replays roles and tablespaces, then every database file.
func restorePostgreSQLServer(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, manifest Manifest, bundle ClientBundle, secretsDir string) error {
	run, err := newPostgreSQLRunner(ctx, target, password, bundle, secretsDir)
	if err != nil {
		return err
	}
	if err := restoreManifestFiles(archiveDir, manifest.Globals, func(r io.Reader) error {
		_, err := run(r, "--dbname=template1")
		return err
	}); err != nil {
		return err
	}
	for _, entry := range manifest.Databases {
		if err := restoreManifestDatabase(ctx, archiveDir, entry, target, password, options, bundle, secretsDir); err != nil {
			return err
		}
	}
	return nil
}

// restoreMySQLServer replays the grants companion, then every database file.
func restoreMySQLServer(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, manifest Manifest, bundle ClientBundle, secretsDir string) error {
	run, err := newMySQLRunner(ctx, target, password, bundle, secretsDir)
	if err != nil {
		return err
	}
	if err := restoreManifestFiles(archiveDir, manifest.Globals, func(r io.Reader) error {
		_, err := run(r)
		return err
	}); err != nil {
		return err
	}
	for _, entry := range manifest.Databases {
		if err := restoreManifestDatabase(ctx, archiveDir, entry, target, password, options, bundle, secretsDir); err != nil {
			return err
		}
	}
	return nil
}

func newPostgreSQLRunner(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, secretsDir string) (func(io.Reader, ...string) ([]byte, error), error) {
	passfile, err := writePostgreSQLPassfile(secretsDir, target, password)
	if err != nil {
		return nil, err
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
	return func(stdin io.Reader, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Env = env
		cmd.Stdin = stdin
		return runClientCommand(cmd, password)
	}, nil
}

func newMySQLRunner(ctx context.Context, target coredb.Target, password string, bundle ClientBundle, secretsDir string) (func(io.Reader, ...string) ([]byte, error), error) {
	defaultsFile, err := writeMySQLDefaultsFile(secretsDir, password)
	if err != nil {
		return nil, err
	}
	baseArgs := append(mySQLBaseArgs(target, defaultsFile), mySQLTLSArgs(target.DatabaseTLSMode, target.DatabaseCACertificate, bundle.Family)...)
	return func(stdin io.Reader, args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, bundle.RestoreProgram, append(baseArgs, args...)...)
		cmd.Stdin = stdin
		return runClientCommand(cmd, password)
	}, nil
}

func restorePostgreSQL(ctx context.Context, dumpPath string, target coredb.Target, password string, options RestoreOptions, scope string, bundle ClientBundle, secretsDir string) error {
	run, err := newPostgreSQLRunner(ctx, target, password, bundle, secretsDir)
	if err != nil {
		return err
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
	run, err := newMySQLRunner(ctx, target, password, bundle, secretsDir)
	if err != nil {
		return err
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

// runClientCommand runs a database client, redacting the password from failures.
func runClientCommand(cmd *exec.Cmd, password string) ([]byte, error) {
	return runClientCommandWithLog(cmd, password, nil)
}

func runClientCommandWithLog(cmd *exec.Cmd, password string, logWriter io.Writer) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	message := stderr.String()
	if password != "" {
		message = strings.ReplaceAll(message, password, "[redacted]")
	}
	if message != "" && logWriter != nil {
		if _, err := io.WriteString(logWriter, message); err != nil && runErr == nil {
			return nil, fmt.Errorf("write database client log: %w", err)
		}
	}
	if runErr != nil {
		return nil, fmt.Errorf("%w: %s", runErr, limitedText(message, 4096))
	}
	return stdout.Bytes(), nil
}

func postgreSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func restoreLDAP(ctx context.Context, archiveDir string, target coredb.Target, password string, options RestoreOptions, manifest Manifest, bundle ClientBundle) error {
	secretsDir, err := os.MkdirTemp("", ".pbs-plus-database-secrets-")
	if err != nil {
		return fmt.Errorf("create database restore secrets directory: %w", err)
	}
	defer os.RemoveAll(secretsDir)
	if err := os.Chmod(secretsDir, 0o700); err != nil {
		return fmt.Errorf("secure database restore secrets directory: %w", err)
	}
	passfile, err := writeLdapPasswordFile(secretsDir, password)
	if err != nil {
		return err
	}
	archiveBase := manifest.Database
	if archiveBase == "" {
		archiveBase = target.LdapBaseDN
	}
	dn := options.SourceDatabase
	if dn == "" {
		dn = archiveBase
	}
	if archiveBase != "" && !ldapDNWithin(dn, archiveBase) {
		return fmt.Errorf("LDAP source DN %q is outside snapshot base DN %q", dn, archiveBase)
	}
	if options.DestinationDatabase != "" && !strings.EqualFold(options.DestinationDatabase, dn) {
		return errors.New("LDAP restores use the DNs recorded in the dump; destination must match the source DN")
	}

	dumpPath, err := manifestFilePath(archiveDir, manifest.DumpFile)
	if err != nil {
		return err
	}
	preparedPath := filepath.Join(secretsDir, dumpNameLdif)
	if err := prepareLdapRestore(dumpPath, preparedPath, dn); err != nil {
		return err
	}
	if options.ReplaceExisting {
		if err := ldapDeleteSubtree(ctx, target, password, dn, bundle, secretsDir); err != nil {
			return err
		}
	}

	dump, err := os.Open(preparedPath)
	if err != nil {
		return fmt.Errorf("open prepared LDAP dump: %w", err)
	}
	defer dump.Close()
	args := append(ldapClientArgs(target, passfile), "-c", "-a")
	cmd := exec.CommandContext(ctx, bundle.RestoreProgram, args...)
	cmd.Env = ldapTLSCommandEnv(target)
	cmd.Stdin = dump
	if _, err := runClientCommand(cmd, password); err != nil {
		return fmt.Errorf("restore LDAP dump: %w", err)
	}
	return nil
}

func ldapDeleteSubtree(ctx context.Context, target coredb.Target, password, dn string, bundle ClientBundle, secretsDir string) error {
	if bundle.DeleteProgram == "" {
		return errors.New("LDAP client bundle cannot delete entries (ldapdelete is unavailable)")
	}
	passfile, err := writeLdapPasswordFile(secretsDir, password)
	if err != nil {
		return err
	}
	args := append(ldapClientArgs(target, passfile), "-r", dn)
	cmd := exec.CommandContext(ctx, bundle.DeleteProgram, args...)
	cmd.Env = ldapTLSCommandEnv(target)
	if _, err := runClientCommand(cmd, password); err != nil {
		if strings.Contains(err.Error(), "No such object") {
			return nil
		}
		return fmt.Errorf("delete LDAP subtree %s: %w", dn, err)
	}
	return nil
}

type ldapRestoreEntry struct {
	offset int64
	size   int64
	depth  int
}

func prepareLdapRestore(dumpPath, outPath, dn string) error {
	dump, err := os.Open(dumpPath)
	if err != nil {
		return fmt.Errorf("open LDAP dump: %w", err)
	}
	defer dump.Close()
	spool, err := os.CreateTemp(filepath.Dir(outPath), ".ldap-restore-")
	if err != nil {
		return fmt.Errorf("create LDAP restore spool: %w", err)
	}
	defer os.Remove(spool.Name())
	defer spool.Close()

	var entry bytes.Buffer
	entries := make([]ldapRestoreEntry, 0)
	flush := func() error {
		data := bytes.Trim(entry.Bytes(), "\r\n")
		entry.Reset()
		if len(data) == 0 {
			return nil
		}
		entryDN, ok := ldapEntryDN(data)
		if !ok {
			if bytes.HasPrefix(data, []byte("version:")) || bytes.HasPrefix(data, []byte("#")) {
				return nil
			}
			return errors.New("LDAP dump contains an entry without a readable DN")
		}
		if !ldapDNWithin(entryDN, dn) {
			return nil
		}
		offset, err := spool.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		data = append(data, '\n', '\n')
		n, err := spool.Write(data)
		if err != nil {
			return err
		}
		entries = append(entries, ldapRestoreEntry{offset: offset, size: int64(n), depth: ldapDNDepth(entryDN)})
		return nil
	}
	reader := bufio.NewReaderSize(dump, 1<<20)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if len(bytes.TrimRight(line, "\r\n")) == 0 {
				if err := flush(); err != nil {
					return fmt.Errorf("prepare LDAP restore: %w", err)
				}
			} else {
				entry.Write(line)
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return fmt.Errorf("read LDAP dump: %w", readErr)
			}
			break
		}
	}
	if err := flush(); err != nil {
		return fmt.Errorf("prepare LDAP restore: %w", err)
	}
	if len(entries) == 0 {
		return fmt.Errorf("no entries under %q are present in the dump", dn)
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].depth < entries[j].depth })
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create prepared LDAP dump: %w", err)
	}
	for _, entry := range entries {
		if _, err := io.Copy(out, io.NewSectionReader(spool, entry.offset, entry.size)); err != nil {
			_ = out.Close()
			return fmt.Errorf("write prepared LDAP dump: %w", err)
		}
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close prepared LDAP dump: %w", err)
	}
	return nil
}

func ldapEntryDN(entry []byte) (string, bool) {
	lines := strings.Split(string(entry), "\n")
	for i, line := range lines {
		line = strings.TrimSuffix(line, "\r")
		name, value, ok := strings.Cut(line, ":")
		if !ok || !strings.EqualFold(name, "dn") {
			continue
		}
		encoded := strings.HasPrefix(value, ":")
		if encoded {
			value = strings.TrimPrefix(value, ":")
		}
		value = strings.TrimPrefix(value, " ")
		for i++; i < len(lines) && strings.HasPrefix(lines[i], " "); i++ {
			value += strings.TrimSuffix(strings.TrimPrefix(lines[i], " "), "\r")
		}
		if !encoded {
			return value, value != ""
		}
		decoded, err := base64.StdEncoding.DecodeString(value)
		return string(decoded), err == nil && len(decoded) > 0
	}
	return "", false
}

func ldapDNDepth(dn string) int {
	depth := 1
	escaped := false
	for _, r := range dn {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
		} else if r == ',' {
			depth++
		}
	}
	return depth
}

func ldapDNWithin(entryDN, base string) bool {
	if base == "" || strings.EqualFold(entryDN, base) {
		return true
	}
	return strings.HasSuffix(strings.ToLower(entryDN), ","+strings.ToLower(base))
}

func postgreSQLIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func mySQLIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}
