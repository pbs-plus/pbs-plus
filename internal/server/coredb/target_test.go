//go:build linux

package coredb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/crypto"
)

func TestSplitTargetTypesMigration(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "migration.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		PRAGMA foreign_keys=ON;
		CREATE TABLE agent_hosts (name TEXT PRIMARY KEY);
		INSERT INTO agent_hosts (name) VALUES ('agent.example');
		CREATE TABLE targets (
			name TEXT PRIMARY KEY,
			path TEXT NOT NULL,
			agent_host TEXT,
			volume_id TEXT,
			volume_type TEXT,
			volume_name TEXT,
			volume_fs TEXT,
			volume_total_bytes INTEGER,
			volume_used_bytes INTEGER,
			volume_free_bytes INTEGER,
			volume_total TEXT,
			volume_used TEXT,
			volume_free TEXT,
			mount_script TEXT NOT NULL DEFAULT '',
			secret_s3 TEXT NOT NULL DEFAULT '',
			FOREIGN KEY (agent_host) REFERENCES agent_hosts(name) ON DELETE CASCADE
		);
		INSERT INTO targets (name, path) VALUES ('local', '/srv/local');
		INSERT INTO targets (name, path, agent_host, volume_id) VALUES ('agent', '', 'agent.example', 'root');
		INSERT INTO targets (name, path, secret_s3) VALUES ('s3', 'https://storage.example.com/bucket', 'encrypted');
	`)
	if err != nil {
		t.Fatal(err)
	}

	migration, err := migrations.ReadFile("migrations/000041_split_target_types.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(migration)); err != nil {
		t.Fatal(err)
	}

	want := map[string][3]string{
		"local": {"filesystem", "local", "/srv/local"},
		"agent": {"filesystem", "agent", ""},
		"s3":    {"s3", "", "https://storage.example.com/bucket"},
	}
	for name, expected := range want {
		var kind, access, path string
		err := db.QueryRow(`
			SELECT t.target_type, COALESCE(f.access, ''), COALESCE(f.path, s.url, '')
			FROM targets t
			LEFT JOIN target_filesystems f ON f.target_name = t.name
			LEFT JOIN target_s3 s ON s.target_name = t.name
			WHERE t.name = ?
		`, name).Scan(&kind, &access, &path)
		if err != nil {
			t.Fatal(err)
		}
		if got := [3]string{kind, access, path}; got != expected {
			t.Errorf("target %q = %v, want %v", name, got, expected)
		}
	}

	down, err := migrations.ReadFile("migrations/000041_split_target_types.down.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(down)); err != nil {
		t.Fatal(err)
	}
	var path, host, secret string
	if err := db.QueryRow("SELECT path, COALESCE(agent_host, ''), secret_s3 FROM targets WHERE name = 's3'").Scan(&path, &host, &secret); err != nil {
		t.Fatal(err)
	}
	if path != "https://storage.example.com/bucket" || host != "" || secret != "encrypted" {
		t.Errorf("rolled-back S3 target = path %q, host %q, secret %q", path, host, secret)
	}
}

func TestTargetTypePersistence(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "targets.db"))
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	host := AgentHost{Name: "agent.example", IP: "192.0.2.1", OperatingSystem: "linux"}
	if err := db.CreateAgentHost(nil, host); err != nil {
		t.Fatal(err)
	}

	targets := []Target{
		{
			Name:   "local",
			Type:   TargetTypeFilesystem,
			Access: FilesystemAccessLocal,
			Path:   t.TempDir(),
		},
		{
			Name:      "agent",
			Type:      TargetTypeFilesystem,
			Access:    FilesystemAccessAgent,
			AgentHost: host,
			VolumeID:  "root",
		},
		{
			Name: "s3",
			Type: TargetTypeS3,
			Path: "https://storage.example.com/bucket",
		},
	}

	for _, target := range targets {
		if err := db.CreateTarget(nil, target); err != nil {
			t.Fatalf("CreateTarget(%q): %v", target.Name, err)
		}

		got, err := db.GetTarget(target.Name)
		if err != nil {
			t.Fatalf("GetTarget(%q): %v", target.Name, err)
		}
		if got.Type != target.Type || got.Access != target.Access || got.Path != target.Path {
			t.Errorf("GetTarget(%q) = type %q, access %q, path %q; want %q, %q, %q", target.Name, got.Type, got.Access, got.Path, target.Type, target.Access, target.Path)
		}
		if target.IsAgent() && got.AgentHost.Name != host.Name {
			t.Errorf("GetTarget(%q) agent = %q, want %q", target.Name, got.AgentHost.Name, host.Name)
		}
		if target.IsS3() && got.S3Info == nil {
			t.Errorf("GetTarget(%q) did not populate S3 metadata", target.Name)
		}
	}

	converted := Target{Name: "local", Type: TargetTypeS3, Path: "https://storage.example.com/converted"}
	if err := db.UpdateTarget(nil, converted); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetTarget(converted.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsS3() || got.Path != converted.Path {
		t.Errorf("converted target = type %q, path %q", got.Type, got.Path)
	}
	var filesystemRows int
	if err := db.Reader().QueryRow("SELECT COUNT(*) FROM target_filesystems WHERE target_name = ?", converted.Name).Scan(&filesystemRows); err != nil {
		t.Fatal(err)
	}
	if filesystemRows != 0 {
		t.Errorf("converted target retained %d filesystem detail rows", filesystemRows)
	}
}

func TestDatabaseTargetAndJobOptionsPersistence(t *testing.T) {
	crypto.SetSealKeyPath(filepath.Join(t.TempDir(), "seal.key"))
	t.Cleanup(func() { crypto.SetSealKeyPath("") })

	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "database-targets.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	postgres := Target{
		Name:                     "postgres",
		Type:                     TargetTypePostgreSQL,
		DatabaseHost:             "postgres.example",
		DatabaseUsername:         "backup",
		DatabaseTLSMode:          "verify-full",
		DatabaseCACertificate:    "/etc/ssl/postgres-ca.pem",
		DatabaseDefaultClientDir: "/usr/lib/postgresql/17/bin",
	}
	mysql := Target{
		Name:                     "mariadb",
		Type:                     TargetTypeMySQL,
		DatabaseVariant:          "mariadb",
		DatabaseHost:             "mariadb.example",
		DatabaseUsername:         "backup",
		DatabaseTLSMode:          "verify-identity",
		DatabaseDefaultClientDir: "/usr/bin",
	}
	for _, target := range []Target{postgres, mysql} {
		if err := db.CreateTarget(nil, target); err != nil {
			t.Fatalf("CreateTarget(%q): %v", target.Name, err)
		}
	}

	gotPostgres, err := db.GetTarget(postgres.Name)
	if err != nil {
		t.Fatal(err)
	}
	if gotPostgres.DatabasePort != 5432 || gotPostgres.DatabaseHost != postgres.DatabaseHost || !gotPostgres.IsDatabase() {
		t.Errorf("PostgreSQL target = %#v", gotPostgres)
	}
	gotMySQL, err := db.GetTarget(mysql.Name)
	if err != nil {
		t.Fatal(err)
	}
	if gotMySQL.DatabasePort != 3306 || gotMySQL.DatabaseClientFamily != "mariadb" || gotMySQL.DatabaseVariant != "mariadb" {
		t.Errorf("MySQL target = %#v", gotMySQL)
	}

	if err := db.AddDatabasePassword(nil, postgres.Name, "secret"); err != nil {
		t.Fatal(err)
	}
	password, err := db.GetDatabasePassword(postgres.Name)
	if err != nil {
		t.Fatal(err)
	}
	if password != "secret" {
		t.Errorf("database password = %q", password)
	}

	backup := Backup{
		ID:                "postgres-backup",
		Store:             "datastore",
		Target:            Target{Name: postgres.Name},
		DatabaseScope:     "database",
		DatabaseName:      "inventory",
		DatabaseClientDir: "/usr/lib/postgresql/16/bin",
	}
	if err := db.CreateBackup(nil, backup); err != nil {
		t.Fatal(err)
	}
	gotBackup, err := db.GetBackup(backup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotBackup.DatabaseScope != "database" || gotBackup.DatabaseName != "inventory" || gotBackup.Target.Type != TargetTypePostgreSQL {
		t.Errorf("database backup = %#v", gotBackup)
	}

	backup.DatabaseScope = "server"
	if err := db.UpdateBackup(nil, backup); err != nil {
		t.Fatal(err)
	}
	gotBackup, err = db.GetBackup(backup.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotBackup.DatabaseScope != "server" || gotBackup.DatabaseName != "" {
		t.Errorf("server scope kept a database name: %#v", gotBackup)
	}

	restore := Restore{
		ID:                   "mariadb-restore",
		Store:                "datastore",
		Snapshot:             "host/snapshot/1",
		DestTarget:           Target{Name: mysql.Name},
		DestinationDatabase:  "inventory_copy",
		ReplaceExisting:      true,
		DatabaseClientFamily: "mysql",
		DatabaseClientDir:    "/opt/mysql/bin",
	}
	if err := db.CreateRestore(nil, restore); err != nil {
		t.Fatal(err)
	}
	gotRestore, err := db.GetRestore(restore.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotRestore.DestinationDatabase != "inventory_copy" || !gotRestore.ReplaceExisting || gotRestore.DestTarget.Type != TargetTypeMySQL {
		t.Errorf("database restore = %#v", gotRestore)
	}
}
