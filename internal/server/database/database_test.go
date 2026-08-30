//go:build linux

package database

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestDiscoverClientBundles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"pg_dump", "pg_dumpall", "psql", "mariadb-dump", "mariadb"} {
		writeTestProgram(t, dir, name, "#!/bin/sh\necho '"+name+" 17.2'\n")
	}

	bundles := discoverClientBundles(context.Background(), []string{dir}, []string{dir})
	if len(bundles) != 2 {
		t.Fatalf("discovered %d bundles, want 2", len(bundles))
	}
	postgres, err := FindClientBundle(bundles, EnginePostgreSQL, FamilyPostgreSQL, dir)
	if err != nil {
		t.Fatal(err)
	}
	if postgres.ServerDumpProgram == "" || !strings.Contains(postgres.Version, "17.2") {
		t.Errorf("PostgreSQL bundle = %#v", postgres)
	}
	if _, err := FindClientBundle(bundles, EngineMySQL, FamilyMySQL, dir); err == nil {
		t.Fatal("found unavailable MySQL bundle")
	}
}

func TestDumpCommandsDoNotLockTarget(t *testing.T) {
	postgres := coredb.Target{Type: coredb.TargetTypePostgreSQL, DatabaseHost: "postgres.example", DatabasePort: 5432, DatabaseUsername: "backup", DatabaseTLSMode: "require"}
	bundle := ClientBundle{Engine: EnginePostgreSQL, Family: FamilyPostgreSQL, DumpProgram: "/usr/bin/pg_dump", ServerDumpProgram: "/usr/bin/pg_dumpall"}
	secrets := t.TempDir()
	cmd, err := postgreSQLGlobalsDumpCommand(context.Background(), postgres, "secret", bundle, secrets, "--roles-only")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(cmd.Path) != "pg_dumpall" {
		t.Errorf("server dump program = %s", cmd.Path)
	}
	if !slices.Contains(cmd.Args, "--lock-wait-timeout=30s") {
		t.Errorf("server dump does not bound its lock wait: %v", cmd.Args)
	}

	mysql := coredb.Target{Type: coredb.TargetTypeMySQL, DatabaseHost: "mysql.example", DatabasePort: 3306, DatabaseUsername: "backup", DatabaseTLSMode: "disabled"}
	mysqlBundle := ClientBundle{Engine: EngineMySQL, Family: FamilyMySQL, DumpProgram: "/usr/bin/mysqldump", ServerDumpProgram: "/usr/bin/mysqldump", RestoreProgram: "/usr/bin/mysql"}
	cmd, err = mySQLDumpCommand(context.Background(), mysql, "secret", DumpOptions{Scope: "database", Database: "inventory"}, mysqlBundle, secrets)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(cmd.Args, "--single-transaction") {
		t.Errorf("mysql dump does not use a consistent snapshot: %v", cmd.Args)
	}
	if slices.Contains(cmd.Args, "--lock-all-tables") {
		t.Errorf("mysql dump locks all tables: %v", cmd.Args)
	}
}

func TestStagePostgreSQLDump(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"pg_dump", "pg_dumpall", "psql"} {
		writeTestProgram(t, dir, name, "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'PostgreSQL 17.2'; exit 0; fi\nprintf 'CREATE TABLE inventory (id integer);\\n'\n")
	}
	bundles := discoverClientBundles(context.Background(), []string{dir}, []string{dir})
	bundle, err := FindClientBundle(bundles, EnginePostgreSQL, FamilyPostgreSQL, dir)
	if err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "require",
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "super-secret", DumpOptions{Scope: "database", Database: "inventory"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	manifest, err := LoadManifest(staged.ArchiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Engine != EnginePostgreSQL || manifest.Database != "inventory" {
		t.Errorf("manifest = %#v", manifest)
	}
	for _, name := range []string{manifestName, dumpName} {
		data, err := os.ReadFile(filepath.Join(staged.ArchiveDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "super-secret") {
			t.Fatalf("%s contains database password", name)
		}
	}

	if err := os.WriteFile(filepath.Join(staged.ArchiveDir, dumpName), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(staged.ArchiveDir); err == nil {
		t.Fatal("accepted database dump with mismatched checksum")
	}
}

func TestMySQLPasswordUsesPrivateOptionFile(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"mysqldump", "mysql"} {
		writeTestProgram(t, dir, name, "#!/bin/sh\necho 'MySQL 8.4'\n")
	}
	bundle := discoverClientBundles(context.Background(), []string{dir}, []string{dir})[0]
	secretsDir := t.TempDir()
	target := coredb.Target{
		Type:                  coredb.TargetTypeMySQL,
		DatabaseHost:          "mysql.example",
		DatabasePort:          3306,
		DatabaseUsername:      "backup",
		DatabaseTLSMode:       "verify-identity",
		DatabaseCACertificate: "/etc/ssl/mysql-ca.pem",
	}
	cmd, err := mySQLDumpCommand(context.Background(), target, "line one\nline two", DumpOptions{Scope: "database", Database: "inventory"}, bundle, secretsDir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(cmd.Args, " "), "line one") {
		t.Fatal("database password appears in command arguments")
	}
	optionFile := ""
	for _, arg := range cmd.Args {
		if after, ok := strings.CutPrefix(arg, "--defaults-extra-file="); ok {
			optionFile = after
		}
	}
	if optionFile == "" {
		t.Fatal("mysql dump does not use a private option file")
	}
	info, err := os.Stat(optionFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("option file mode = %o", info.Mode().Perm())
	}
}

func TestRestorePostgreSQLRequiresExplicitReplacement(t *testing.T) {
	dir := t.TempDir()
	dumpProgram := filepath.Join(dir, "pg_dump")
	restoreProgram := filepath.Join(dir, "psql")
	writeTestProgram(t, dir, "pg_dump", "#!/bin/sh\nprintf 'CREATE TABLE inventory (id integer);\\n'\n")
	writeTestProgram(t, dir, "psql", `#!/bin/sh
printf 'PGPASSFILE=%s ARGS=%s\n' "$PGPASSFILE" "$*" >> "$DATABASE_TEST_LOG"
case "$*" in
  *"SELECT 1 FROM pg_database"*) printf '1\n' ;;
  *"--command="*) ;;
  *) cat > "$DATABASE_RESTORE_INPUT" ;;
esac
`)
	logPath := filepath.Join(dir, "restore.log")
	inputPath := filepath.Join(dir, "restore.sql")
	t.Setenv("DATABASE_TEST_LOG", logPath)
	t.Setenv("DATABASE_RESTORE_INPUT", inputPath)

	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "require",
	}
	bundle := ClientBundle{
		Engine:            EnginePostgreSQL,
		Family:            FamilyPostgreSQL,
		DumpProgram:       dumpProgram,
		ServerDumpProgram: dumpProgram,
		RestoreProgram:    restoreProgram,
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{
		Scope:    "database",
		Database: "inventory",
	}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	options := RestoreOptions{DestinationDatabase: "inventory_copy"}
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", options, bundle); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("restore without replacement error = %v", err)
	}
	options.ReplaceExisting = true
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", options, bundle); err != nil {
		t.Fatal(err)
	}

	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logData)
	for _, command := range []string{`DROP DATABASE "inventory_copy"`, `CREATE DATABASE "inventory_copy"`} {
		if !strings.Contains(logText, command) {
			t.Errorf("restore command log does not contain %q: %s", command, logText)
		}
	}
	for line := range strings.SplitSeq(strings.TrimSpace(logText), "\n") {
		field := strings.Fields(line)[0]
		passfile := strings.TrimPrefix(field, "PGPASSFILE=")
		if _, err := os.Stat(passfile); !os.IsNotExist(err) {
			t.Fatalf("PostgreSQL password file still exists: %s", passfile)
		}
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "CREATE TABLE inventory") {
		t.Fatalf("restore input = %q", input)
	}
}

func TestRestoreMySQLDatabase(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "mysqldump", "#!/bin/sh\nprintf 'CREATE TABLE inventory (id integer);\\n'\n")
	writeTestProgram(t, dir, "mysql", `#!/bin/sh
printf '%s\n' "$*" >> "$DATABASE_TEST_LOG"
case "$*" in
  *"INFORMATION_SCHEMA.SCHEMATA"*) ;;
  *"--execute="*) ;;
  *) cat > "$DATABASE_RESTORE_INPUT" ;;
esac
`)
	logPath := filepath.Join(dir, "restore.log")
	inputPath := filepath.Join(dir, "restore.sql")
	t.Setenv("DATABASE_TEST_LOG", logPath)
	t.Setenv("DATABASE_RESTORE_INPUT", inputPath)
	target := coredb.Target{
		Type:             coredb.TargetTypeMySQL,
		DatabaseHost:     "mysql.example",
		DatabasePort:     3306,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "disabled",
	}
	bundle := ClientBundle{
		Engine:            EngineMySQL,
		Family:            FamilyMySQL,
		DumpProgram:       filepath.Join(dir, "mysqldump"),
		ServerDumpProgram: filepath.Join(dir, "mysqldump"),
		RestoreProgram:    filepath.Join(dir, "mysql"),
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{Scope: "database", Database: "inventory"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{DestinationDatabase: "inventory_copy"}, bundle); err != nil {
		t.Fatal(err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logData), "CREATE DATABASE `inventory_copy`") {
		t.Fatalf("restore command log = %s", logData)
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "CREATE TABLE inventory") {
		t.Fatalf("restore input = %q", input)
	}
}

func TestRestoreDumpRejectsEngineMismatch(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "pg_dump", "#!/bin/sh\nprintf 'SELECT 1;\\n'\n")
	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
	}
	bundle := ClientBundle{
		Engine:            EnginePostgreSQL,
		Family:            FamilyPostgreSQL,
		DumpProgram:       filepath.Join(dir, "pg_dump"),
		ServerDumpProgram: filepath.Join(dir, "pg_dump"),
		RestoreProgram:    filepath.Join(dir, "pg_dump"),
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{Scope: "database", Database: "inventory"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()
	target.Type = coredb.TargetTypeMySQL
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{DestinationDatabase: "inventory"}, bundle); err == nil {
		t.Fatal("restored PostgreSQL dump to MySQL target")
	}
}

func TestStagePostgreSQLServerDump(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "pg_dump", `#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  broken) echo 'pg_dump: broken' >&2; exit 1 ;;
  *) printf 'CREATE TABLE [%s] (id integer);\n' "$last" ;;
esac
`)
	writeTestProgram(t, dir, "pg_dumpall", `#!/bin/sh
case "$*" in
  *--roles-only*) printf 'CREATE ROLE app;\n' ;;
  *) printf 'CREATE TABLESPACE fast;\n' ;;
esac
`)
	writeTestProgram(t, dir, "psql", `#!/bin/sh
case "$*" in
  *"SELECT datname"*) printf 'payroll\nbroken\ninv entory\n' ;;
  *) printf 'ignored\n' ;;
esac
`)
	bundles := discoverClientBundles(context.Background(), []string{dir}, []string{dir})
	bundle, err := FindClientBundle(bundles, EnginePostgreSQL, FamilyPostgreSQL, dir)
	if err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "require",
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "super-secret", DumpOptions{Scope: "server"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	manifest, err := LoadManifest(staged.ArchiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != ManifestVersionV2 {
		t.Errorf("manifest version = %d", manifest.Version)
	}
	if len(manifest.Databases) != 2 || manifest.Databases[0].Name != "payroll" || manifest.Databases[1].Name != "inv entory" {
		t.Errorf("manifest databases = %#v", manifest.Databases)
	}
	if manifest.Databases[1].File != "databases/0003-inv_entory.sql" {
		t.Errorf("database file name = %q", manifest.Databases[1].File)
	}
	if len(manifest.Failed) != 1 || manifest.Failed[0] != "broken" {
		t.Errorf("manifest failed = %#v", manifest.Failed)
	}
	if len(manifest.Globals) != 2 || manifest.Globals[0].File != "roles.sql" || manifest.Globals[1].File != "globals.sql" {
		t.Errorf("manifest globals = %#v", manifest.Globals)
	}
	for file, want := range map[string]string{
		"roles.sql":                  "CREATE ROLE app;",
		"globals.sql":                "CREATE TABLESPACE fast;",
		"databases/0001-payroll.sql": "CREATE TABLE [payroll]",
	} {
		data, err := os.ReadFile(filepath.Join(staged.ArchiveDir, file))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), want) {
			t.Errorf("%s = %q, want %q", file, data, want)
		}
	}
	assertNoFileContainsPassword(t, staged.ArchiveDir, "super-secret")
}

func TestStageMariaDBServerDump(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "mariadb-dump", `#!/bin/sh
case " $* " in
  *" role_edges "*|*" default_roles "*) printf '%s\n' 'mariadb-dump: Couldn'"'"'t find table: "role_edges"' >&2; exit 6 ;;
esac
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  roles_mapping) printf 'INSERT INTO user VALUES (1);\n' ;;
  test) printf 'CREATE TABLE kept (id integer);\n' ;;
  other) printf 'CREATE TABLE dropped (id integer);\n' ;;
esac
`)
	writeTestProgram(t, dir, "mariadb", `#!/bin/sh
case "$*" in
  *"SHOW DATABASES"*) printf 'information_schema\nmysql\nother\nperformance_schema\nsys\ntest\n' ;;
  *"SHOW TABLES FROM mysql"*) printf 'columns_priv\ndb\nglobal_priv\nprocs_priv\nproxies_priv\nroles_mapping\ntables_priv\nuser\n' ;;
  *) printf 'ignored\n' ;;
esac
`)
	bundle := discoverClientBundles(context.Background(), []string{dir}, []string{dir})[0]
	target := coredb.Target{
		Type:             coredb.TargetTypeMySQL,
		DatabaseHost:     "mysql.example",
		DatabasePort:     3306,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "disabled",
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "super-secret", DumpOptions{Scope: "server"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	manifest, err := LoadManifest(staged.ArchiveDir)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != ManifestVersionV2 {
		t.Errorf("manifest version = %d", manifest.Version)
	}
	if len(manifest.Databases) != 2 || manifest.Databases[0].Name != "other" || manifest.Databases[1].Name != "test" {
		t.Errorf("manifest databases = %#v, want system schemas excluded", manifest.Databases)
	}
	if len(manifest.Failed) != 0 {
		t.Errorf("manifest failed = %#v", manifest.Failed)
	}
	if len(manifest.Globals) != 1 || manifest.Globals[0].Name != "grants" || manifest.Globals[0].File != "grants.sql" {
		t.Errorf("manifest globals = %#v", manifest.Globals)
	}
	grants, err := os.ReadFile(filepath.Join(staged.ArchiveDir, "grants.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(grants), "INSERT INTO user") {
		t.Errorf("grants.sql = %q", grants)
	}
	assertNoFileContainsPassword(t, staged.ArchiveDir, "super-secret")
}

func TestRestorePostgreSQLRenamesDatabaseFromV2ServerDump(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "pg_dump", `#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
printf 'CREATE TABLE [%s] (id integer);\n' "$last"
`)
	writeTestProgram(t, dir, "pg_dumpall", `#!/bin/sh
printf 'CREATE ROLE app;\n'
`)
	writeTestProgram(t, dir, "psql", `#!/bin/sh
case "$*" in
  *"SELECT datname"*) printf 'payroll\ninv entory\n' ;;
  *"SELECT 1 FROM pg_database"*) printf '1\n' ;;
  *"--command="*) printf '%s\n' "$*" >> "$DATABASE_TEST_LOG" ;;
  *) printf '%s\n' "$*" >> "$DATABASE_TEST_LOG"
     cat > "$DATABASE_RESTORE_INPUT" ;;
esac
`)
	logPath := filepath.Join(dir, "restore.log")
	inputPath := filepath.Join(dir, "restore.sql")
	t.Setenv("DATABASE_TEST_LOG", logPath)
	t.Setenv("DATABASE_RESTORE_INPUT", inputPath)

	bundles := discoverClientBundles(context.Background(), []string{dir}, []string{dir})
	bundle, err := FindClientBundle(bundles, EnginePostgreSQL, FamilyPostgreSQL, dir)
	if err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "require",
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{Scope: "server"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	options := RestoreOptions{SourceDatabase: "inv entory", DestinationDatabase: "database2", ReplaceExisting: true}
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", options, bundle); err != nil {
		t.Fatal(err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logData)
	for _, command := range []string{`DROP DATABASE "database2"`, `CREATE DATABASE "database2"`, `--dbname=database2`} {
		if !strings.Contains(logText, command) {
			t.Errorf("restore command log does not contain %q: %s", command, logText)
		}
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "CREATE TABLE [inv entory]") || strings.Contains(string(input), "CREATE TABLE [payroll]") {
		t.Fatalf("restore input = %q", input)
	}
}

func TestRestoreMySQLWholeServerFromV2Dump(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "mysqldump", `#!/bin/sh
last=""
for a in "$@"; do last="$a"; done
case "$last" in
  default_roles) printf '%s\n' '-- grants' ;;
  test) printf 'CREATE TABLE kept (id integer);\n' ;;
  other) printf 'CREATE TABLE dropped (id integer);\n' ;;
esac
`)
	writeTestProgram(t, dir, "mysql", `#!/bin/sh
case "$*" in
  *"SHOW DATABASES"*) printf 'other\ntest\n' ;;
  *"SHOW TABLES FROM mysql"*) printf 'columns_priv\ndb\ndefault_roles\nprocs_priv\nproxies_priv\nrole_edges\ntables_priv\nuser\n' ;;
  *"INFORMATION_SCHEMA.SCHEMATA"*) printf '1\n' ;;
  *"--execute="*) printf '%s\n' "$*" >> "$DATABASE_TEST_LOG" ;;
  *) printf '%s\n' "$*" >> "$DATABASE_TEST_LOG"
     cat >> "$DATABASE_RESTORE_INPUT" ;;
esac
`)
	logPath := filepath.Join(dir, "restore.log")
	inputPath := filepath.Join(dir, "restore.sql")
	t.Setenv("DATABASE_TEST_LOG", logPath)
	t.Setenv("DATABASE_RESTORE_INPUT", inputPath)

	bundle := discoverClientBundles(context.Background(), []string{dir}, []string{dir})[0]
	target := coredb.Target{
		Type:             coredb.TargetTypeMySQL,
		DatabaseHost:     "mysql.example",
		DatabasePort:     3306,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "disabled",
	}
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{Scope: "server"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()

	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", RestoreOptions{ReplaceExisting: true}, bundle); err != nil {
		t.Fatal(err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logData)
	for _, command := range []string{"DROP DATABASE `test`", "CREATE DATABASE `test`", "DROP DATABASE `other`", "CREATE DATABASE `other`", "--database=test", "--database=other"} {
		if !strings.Contains(logText, command) {
			t.Errorf("restore command log does not contain %q: %s", command, logText)
		}
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	inputText := string(input)
	lastOrder := -1
	for _, marker := range []string{"-- grants", "CREATE TABLE dropped", "CREATE TABLE kept"} {
		index := strings.Index(inputText, marker)
		if index < 0 {
			t.Errorf("restore input is missing %q: %q", marker, inputText)
			continue
		}
		if index < lastOrder {
			t.Errorf("restore input has %q before earlier content: %q", marker, inputText)
		}
		lastOrder = index
	}
}

func TestRestoreV1ServerDumpStillRenames(t *testing.T) {
	dir := t.TempDir()
	dump := strings.Join([]string{
		"-- Current Database: `test`",
		"CREATE DATABASE `test`;",
		"USE `test`;",
		"CREATE TABLE kept (id integer);",
		"-- Current Database: `other`",
		"CREATE TABLE dropped (id integer);",
		"",
	}, "\n")
	archiveDir := filepath.Join(dir, "archive")
	if err := os.MkdirAll(archiveDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, dumpName), []byte(dump), 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(dump))
	manifest := Manifest{
		Version:    ManifestVersionV1,
		Engine:     EngineMySQL,
		Scope:      "server",
		DumpFile:   dumpName,
		DumpSHA256: hex.EncodeToString(digest[:]),
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}

	writeTestProgram(t, dir, "mysql", `#!/bin/sh
printf '%s\n' "$*" >> "$DATABASE_TEST_LOG"
case "$*" in
  *"INFORMATION_SCHEMA.SCHEMATA"*) ;;
  *"--execute="*) ;;
  *) cat > "$DATABASE_RESTORE_INPUT" ;;
esac
`)
	logPath := filepath.Join(dir, "restore.log")
	inputPath := filepath.Join(dir, "restore.sql")
	t.Setenv("DATABASE_TEST_LOG", logPath)
	t.Setenv("DATABASE_RESTORE_INPUT", inputPath)
	target := coredb.Target{
		Type:             coredb.TargetTypeMySQL,
		DatabaseHost:     "mysql.example",
		DatabasePort:     3306,
		DatabaseUsername: "backup",
		DatabaseTLSMode:  "disabled",
	}
	bundle := ClientBundle{
		Engine:            EngineMySQL,
		Family:            FamilyMySQL,
		DumpProgram:       filepath.Join(dir, "mysqldump"),
		ServerDumpProgram: filepath.Join(dir, "mysqldump"),
		RestoreProgram:    filepath.Join(dir, "mysql"),
	}
	options := RestoreOptions{SourceDatabase: "test", DestinationDatabase: "database2"}
	if err := RestoreDump(context.Background(), archiveDir, target, "secret", options, bundle); err != nil {
		t.Fatal(err)
	}
	logData, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logData), "CREATE DATABASE `database2`") || !strings.Contains(string(logData), "--database=database2") {
		t.Fatalf("restore did not target the renamed database: %s", logData)
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "CREATE TABLE kept") || strings.Contains(string(input), "CREATE TABLE dropped") {
		t.Fatalf("restore input = %s", input)
	}
}

func TestLoadManifestRejectsV2LayoutAbuses(t *testing.T) {
	archiveDir := t.TempDir()
	dump := []byte("-- dump\n")
	if err := os.WriteFile(filepath.Join(archiveDir, dumpName), dump, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(dump)
	base := Manifest{
		Version:   ManifestVersionV2,
		Engine:    EngineMySQL,
		Scope:     "server",
		Databases: []ManifestFile{{Name: "evil", File: "../../" + dumpName, SHA256: hex.EncodeToString(digest[:])}},
	}
	data, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(archiveDir); err == nil {
		t.Fatal("accepted a manifest file path escaping the archive directory")
	} else if !strings.Contains(err.Error(), "invalid file path") {
		t.Fatalf("escape error = %v", err)
	}

	base.Databases = []ManifestFile{{Name: "ok", File: "databases/0001-ok.sql", SHA256: hex.EncodeToString(digest[:])}}
	base.Scope = "database"
	base.Database = "ok"
	data, err = json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(archiveDir, manifestName), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadManifest(archiveDir); err == nil {
		t.Fatal("accepted a v2 manifest outside server scope")
	} else if !strings.Contains(err.Error(), "require server scope") {
		t.Fatalf("scope error = %v", err)
	}
}

func assertNoFileContainsPassword(t *testing.T, root, password string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), password) {
			t.Errorf("%s contains the database password", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCopyDumpSectionExtractsOneDatabase(t *testing.T) {
	postgres := strings.Join([]string{
		"CREATE ROLE app;",
		`\connect payroll`,
		"CREATE TABLE wages (id int);",
		`\connect "inv entory"`,
		"CREATE TABLE parts (id int);",
		"COPY parts (id) FROM stdin;",
		`\\connect payroll`,
		`\.`,
		`\connect scratch`,
		"CREATE TABLE junk (id int);",
		"",
	}, "\n")
	mysql := strings.Join([]string{
		"-- Current Database: `payroll`",
		"CREATE DATABASE `payroll`;",
		"USE `payroll`;",
		"CREATE TABLE wages (id int);",
		"-- Current Database: `inventory`",
		"CREATE DATABASE /*!32312 IF NOT EXISTS*/ `inventory`;",
		"USE `inventory`;",
		"CREATE TABLE parts (id int);",
		"-- Current Database: `scratch`",
		"CREATE TABLE junk (id int);",
		"",
	}, "\n")

	for _, testCase := range []struct{ name, engine, dump, database string }{
		{"postgresql", EnginePostgreSQL, postgres, "inv entory"},
		{"mysql", EngineMySQL, mysql, "inventory"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var out strings.Builder
			if err := copyDumpSection(&out, bufio.NewReader(strings.NewReader(testCase.dump)), testCase.engine, testCase.database); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), "CREATE TABLE parts") {
				t.Errorf("selected database is missing from the extract: %q", out.String())
			}
			for _, unwanted := range []string{"CREATE TABLE wages", "CREATE TABLE junk", "CREATE DATABASE", "USE `"} {
				if strings.Contains(out.String(), unwanted) {
					t.Errorf("extract leaked %q: %q", unwanted, out.String())
				}
			}
			err := copyDumpSection(io.Discard, bufio.NewReader(strings.NewReader(testCase.dump)), testCase.engine, "absent")
			if err == nil || !strings.Contains(err.Error(), "not present in the dump") {
				t.Fatalf("missing database was not reported: %v", err)
			}
		})
	}
}

func writeTestProgram(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
}
