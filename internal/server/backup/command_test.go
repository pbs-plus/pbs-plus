//go:build linux

package backup

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs/jobdb"
	"github.com/pbs-plus/pbs-plus/internal/server/plugins"
	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestPluginBackupCommandPolicy(t *testing.T) {
	lease := &plugins.BackupLease{}
	mode, useExclusions := backupCommandPolicy(coredb.Backup{Mode: "legacy"}, lease)
	if mode != "--change-detection-mode=metadata" || useExclusions {
		t.Fatalf("basic plugin policy = %q, %v", mode, useExclusions)
	}
	lease.HostFeatures = []targetplugin.HostFeature{targetplugin.FeatureChangeDetection, targetplugin.FeatureExclusions}
	mode, useExclusions = backupCommandPolicy(coredb.Backup{Mode: "legacy"}, lease)
	if mode != "--change-detection-mode=legacy" || !useExclusions {
		t.Fatalf("full plugin policy = %q, %v", mode, useExclusions)
	}
}

func TestPluginBackupIncludesSnapshotMetadataArchive(t *testing.T) {
	backup := coredb.Backup{Target: coredb.Target{Name: "plugin-target"}}
	lease := &plugins.BackupLease{MetadataSourcePath: "/metadata"}
	sources, err := backupSourceArgs(backup, "/source", lease)
	if err != nil {
		t.Fatalf("backupSourceArgs: %v", err)
	}
	if len(sources) != 2 || sources[0] != "plugin-target.pxar:/source" || sources[1] != targetplugin.SnapshotMetadataArchiveName+".pxar:/metadata" {
		t.Fatalf("plugin backup sources = %#v", sources)
	}
	if _, err := backupSourceArgs(backup, "/source", &plugins.BackupLease{}); err == nil {
		t.Fatal("plugin backup without metadata source accepted")
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
