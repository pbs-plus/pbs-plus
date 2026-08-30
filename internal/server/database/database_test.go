//go:build linux

package database

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
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
	cmd, err := mySQLDumpCommand(context.Background(), target, "line one\nline two", DumpOptions{Scope: "server"}, bundle, secretsDir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(cmd.Args, " "), "line one") {
		t.Fatal("database password appears in command arguments")
	}
	optionFile := strings.TrimPrefix(cmd.Args[1], "--defaults-extra-file=")
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

func TestRestoreMySQLRenamesDatabaseFromServerDump(t *testing.T) {
	dir := t.TempDir()
	writeTestProgram(t, dir, "mysqldump", "#!/bin/sh\nprintf -- '-- Current Database: `test`\\nCREATE DATABASE `test`;\\nUSE `test`;\\nCREATE TABLE kept (id integer);\\n-- Current Database: `other`\\nCREATE TABLE dropped (id integer);\\n'\n")
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
	staged, err := StageDump(context.Background(), t.TempDir(), target, "secret", DumpOptions{Scope: "server"}, bundle)
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Cleanup()
	options := RestoreOptions{SourceDatabase: "test", DestinationDatabase: "database2", ReplaceExisting: true}
	if err := RestoreDump(context.Background(), staged.ArchiveDir, target, "secret", options, bundle); err != nil {
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
