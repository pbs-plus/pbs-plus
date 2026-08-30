//go:build linux

package database

import (
	"context"
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

func writeTestProgram(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o700); err != nil {
		t.Fatal(err)
	}
}
