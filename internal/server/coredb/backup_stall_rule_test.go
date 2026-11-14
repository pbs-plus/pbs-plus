//go:build linux

package coredb

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestStallRuleMigrationBackfillsExistingJobs(t *testing.T) {
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exec := func(stmt string) {
		t.Helper()
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	migration := func(name string) string {
		t.Helper()
		b, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	exec(`CREATE TABLE backups (id TEXT PRIMARY KEY, store TEXT NOT NULL)`)
	exec(`INSERT INTO backups (id, store) VALUES ('legacy-job', 'store')`)
	exec(migration("000052_add_backup_stall_rule.up.sql"))

	var timeout, minProgress int
	if err := db.QueryRow(`SELECT stall_timeout, stall_min_progress FROM backups WHERE id = 'legacy-job'`).Scan(&timeout, &minProgress); err != nil {
		t.Fatal(err)
	}
	if timeout != DefaultStallTimeout || minProgress != DefaultStallMinProgress {
		t.Fatalf("existing job got stall rule (%d, %d), want (%d, %d)", timeout, minProgress, DefaultStallTimeout, DefaultStallMinProgress)
	}

	exec(migration("000052_add_backup_stall_rule.down.sql"))
	if _, err := db.Exec(`SELECT stall_timeout FROM backups`); err == nil {
		t.Fatal("down migration left stall_timeout in place")
	}
}

func TestStallRuleRoundTrip(t *testing.T) {
	db, err := Initialize(context.Background(), filepath.Join(t.TempDir(), "stall-rule.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	target := Target{Name: "local", Type: TargetTypeFilesystem, Access: FilesystemAccessLocal, Path: t.TempDir()}
	if err := db.CreateTarget(nil, target); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		id                   string
		timeout, minProgress int
		wantTimeout, wantMin int
	}{
		{"unset", 0, 0, DefaultStallTimeout, DefaultStallMinProgress},
		{"custom", 30, 256, 30, 256},
		{"disabled", StallTimeoutDisabled, 0, StallTimeoutDisabled, DefaultStallMinProgress},
		{"invalid", -7, -1, DefaultStallTimeout, DefaultStallMinProgress},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			b := Backup{ID: tc.id, Store: "store", Target: target, StallTimeout: tc.timeout, StallMinProgress: tc.minProgress}
			if err := db.CreateBackup(nil, b); err != nil {
				t.Fatal(err)
			}
			got, err := db.GetBackup(tc.id)
			if err != nil {
				t.Fatal(err)
			}
			if got.StallTimeout != tc.wantTimeout || got.StallMinProgress != tc.wantMin {
				t.Fatalf("created stall rule = (%d, %d), want (%d, %d)", got.StallTimeout, got.StallMinProgress, tc.wantTimeout, tc.wantMin)
			}

			got.StallTimeout, got.StallMinProgress = 0, 0
			if err := db.UpdateBackup(nil, got); err != nil {
				t.Fatal(err)
			}
			got, err = db.GetBackup(tc.id)
			if err != nil {
				t.Fatal(err)
			}
			if got.StallTimeout != DefaultStallTimeout || got.StallMinProgress != DefaultStallMinProgress {
				t.Fatalf("updated stall rule = (%d, %d), want defaults", got.StallTimeout, got.StallMinProgress)
			}
		})
	}
}
