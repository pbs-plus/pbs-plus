//go:build linux

package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
)

func TestTaskLogWriterMirrorsDatabaseOutput(t *testing.T) {
	var destination bytes.Buffer
	var queued []string
	writer := taskLogWriter{
		destination: &destination,
		logLine: func(line string) {
			queued = append(queued, line)
		},
	}
	input := []byte("first line\nsecond line\n")
	written, err := writer.Write(input)
	if err != nil {
		t.Fatal(err)
	}
	if written != len(input) || destination.String() != string(input) {
		t.Fatalf("destination = %q, bytes = %d", destination.String(), written)
	}
	if len(queued) != 2 || queued[0] != "first line" || queued[1] != "second line" {
		t.Fatalf("queued lines = %#v", queued)
	}
}

func TestDatabaseBackupCommandPolicy(t *testing.T) {
	mode, useExclusions := backupCommandPolicy(coredb.Backup{
		Mode: "legacy",
		Target: coredb.Target{
			Type: coredb.TargetTypePostgreSQL,
		},
	})
	if mode != "--change-detection-mode=metadata" {
		t.Fatalf("database change detection mode = %q", mode)
	}
	if useExclusions {
		t.Fatal("database backup accepted PXAR exclusions")
	}
}

func TestRegisterSelectsBackupWorkflowVersion2(t *testing.T) {
	db, err := jobdb.Open(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine, err := jobs.NewEngine(db, jobs.EngineConfig{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := Register(engine, nil); err != nil {
		t.Fatal(err)
	}
	if err := engine.RegisterVersion(jobs.WorkflowBackup, "1", func(*jobs.WorkflowContext) error { return nil }); err == nil {
		t.Fatal("backup workflow version 1 was not retained")
	}
	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowBackup, "backup-id", "test", "backup-v2", jobs.BackupInput{}, nil, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	execution, _, err := engine.Submit(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if execution.WorkflowVersion != "2" {
		t.Fatalf("current backup workflow version = %q", execution.WorkflowVersion)
	}
}

func TestBackupCleanupRemovesDatabaseStaging(t *testing.T) {
	dir := t.TempDir()
	dumpProgram := filepath.Join(dir, "pg_dump")
	if err := os.WriteFile(dumpProgram, []byte("#!/bin/sh\nprintf 'SELECT 1;\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	target := coredb.Target{
		Type:             coredb.TargetTypePostgreSQL,
		DatabaseHost:     "postgres.example",
		DatabasePort:     5432,
		DatabaseUsername: "backup",
	}
	staged, err := database.StageDump(context.Background(), "", target, "secret", database.DumpOptions{
		Scope:    "database",
		Database: "inventory",
	}, database.ClientBundle{
		Engine:            database.EnginePostgreSQL,
		Family:            database.FamilyPostgreSQL,
		DumpProgram:       dumpProgram,
		ServerDumpProgram: dumpProgram,
		RestoreProgram:    dumpProgram,
	})
	if err != nil {
		t.Fatal(err)
	}
	archiveDir := staged.ArchiveDir
	b := &backupJob{waitGroup: &sync.WaitGroup{}, stagedDump: staged}
	b.cleanup()
	if _, err := os.Stat(archiveDir); !os.IsNotExist(err) {
		t.Fatalf("database staging directory still exists: %v", err)
	}
}
