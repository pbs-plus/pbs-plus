//go:build linux

package application

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestGetStatusEntriesMerge(t *testing.T) {
	service := NewTargetService(nil, arpc.NewAgentsManager())
	localPath := t.TempDir()
	targets := []coredb.Target{
		{Name: "cached", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessAgent, AgentHost: coredb.AgentHost{Name: "offline"}},
		{Name: "agent-no-session", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessAgent, AgentHost: coredb.AgentHost{Name: "offline2"}},
		{Name: "local-ok", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: localPath},
		{Name: "local-missing", Type: coredb.TargetTypeFilesystem, Access: coredb.FilesystemAccessLocal, Path: localPath + "/missing"},
		{Name: "s3-no-cache", Type: coredb.TargetTypeS3, S3Info: &coredb.S3Url{Endpoint: "127.0.0.1:1"}},
	}
	service.statusCache.Set("cached", StatusEntry{ConnectionStatus: new(true), AgentVersion: "9.9", CheckedAt: time.Now()})

	entries := service.GetStatusEntries(targets)

	if e := entries["cached"]; e.ConnectionStatus == nil || !*e.ConnectionStatus || e.AgentVersion != "9.9" {
		t.Errorf("cached entry = %#v", e)
	}
	if e := entries["agent-no-session"]; e.ConnectionStatus != nil || e.AgentVersion != "" {
		t.Errorf("agent without session or cache = %#v", e)
	}
	if e := entries["local-ok"]; e.ConnectionStatus == nil || !*e.ConnectionStatus || e.VolumeTotalBytes <= 0 || e.CheckedAt.IsZero() {
		t.Errorf("local ok entry = %#v", e)
	}
	if e := entries["local-missing"]; e.ConnectionStatus == nil || *e.ConnectionStatus || e.LastError == "" {
		t.Errorf("local missing entry = %#v", e)
	}
	if e := entries["s3-no-cache"]; e.ConnectionStatus != nil {
		t.Errorf("never-probed s3 entry = %#v", e)
	}
}

func TestGetStatusEntriesRevalidatesStaleAndEvicts(t *testing.T) {
	dir := t.TempDir()
	crypto.SetSealKeyPath(filepath.Join(dir, "seal.key"))
	db, err := coredb.Initialize(t.Context(), filepath.Join(dir, "targets.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	service := NewTargetService(db, arpc.NewAgentsManager())
	localPath := t.TempDir()
	for _, name := range []string{"local-ok", "local-missing"} {
		path := localPath
		if name == "local-missing" {
			path += "/missing"
		}
		if err := service.CreateTarget(nil, coredb.Target{
			Name:   name,
			Type:   coredb.TargetTypeFilesystem,
			Access: coredb.FilesystemAccessLocal,
			Path:   path,
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Stale entry is served as-is, then refreshed by the background probe.
	service.statusCache.Set("local-ok", StatusEntry{ConnectionStatus: new(false), CheckedAt: time.Now().Add(-time.Minute)})
	targets, err := service.GetAllTargets()
	if err != nil {
		t.Fatal(err)
	}
	entries := service.GetStatusEntries(targets)
	if entries["local-ok"].ConnectionStatus == nil || *entries["local-ok"].ConnectionStatus {
		t.Fatal("stale entry should be served unchanged")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		okEntry, okFound := service.statusCache.Get("local-ok")
		_, missingFound := service.statusCache.Get("local-missing")
		if okFound && missingFound && okEntry.ConnectionStatus != nil && *okEntry.ConnectionStatus && time.Since(okEntry.CheckedAt) < statusRevalidateAfter {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stale entries not revalidated: local-ok=%#v local-missing-cached=%v", okEntry, missingFound)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Deleting a target evicts its cache entry on the next read.
	if err := service.DeleteTarget(nil, "local-ok"); err != nil {
		t.Fatal(err)
	}
	remaining, err := service.GetAllTargets()
	if err != nil {
		t.Fatal(err)
	}
	service.GetStatusEntries(remaining)
	if _, found := service.statusCache.Get("local-ok"); found {
		t.Error("evicted target still cached")
	}
	if _, found := service.statusCache.Get("local-missing"); !found {
		t.Error("live target wrongly evicted")
	}
}

func TestStoreStatusResultsTimeoutKeepsLastVerdict(t *testing.T) {
	service := NewTargetService(nil, arpc.NewAgentsManager())
	targets := []coredb.Target{{Name: "t1"}}

	reachableAt := time.Now().Add(-time.Minute)
	service.statusCache.Set("t1", StatusEntry{ConnectionStatus: new(true), CheckedAt: reachableAt})
	service.storeStatusResults(targets, []TargetStatusResult{{Index: 0, Error: context.DeadlineExceeded}})
	e, _ := service.statusCache.Get("t1")
	if e.ConnectionStatus == nil || !*e.ConnectionStatus || !e.CheckedAt.Equal(reachableAt) {
		t.Fatalf("timeout overwrote last verdict: %#v", e)
	}

	service.statusCache.Del("t1")
	service.storeStatusResults(targets, []TargetStatusResult{{Index: 0, Error: context.DeadlineExceeded}})
	e, _ = service.statusCache.Get("t1")
	if e.ConnectionStatus != nil {
		t.Fatalf("timeout with no prior verdict = %#v", e)
	}

	service.statusCache.Set("t1", StatusEntry{ConnectionStatus: new(true), CheckedAt: time.Now()})
	service.storeStatusResults(targets, []TargetStatusResult{{Index: 0, Error: errors.New("connection refused")}})
	e, _ = service.statusCache.Get("t1")
	if e.ConnectionStatus == nil || *e.ConnectionStatus {
		t.Fatalf("explicit error should overwrite: %#v", e)
	}
}

func TestStoreStatusResultsKeepsMetadataOnFailure(t *testing.T) {
	service := NewTargetService(nil, arpc.NewAgentsManager())
	targets := []coredb.Target{{Name: "t1"}}

	service.statusCache.Set("t1", StatusEntry{
		ConnectionStatus: new(true),
		AgentVersion:     "9.9",
		VolumeTotalBytes: 100, VolumeUsedBytes: 40, VolumeFreeBytes: 60,
		CheckedAt: time.Now().Add(-time.Minute),
	})
	service.storeStatusResults(targets, []TargetStatusResult{
		{Index: 0, Error: errors.New("connection refused")},
	})
	e, _ := service.statusCache.Get("t1")
	if e.ConnectionStatus == nil || *e.ConnectionStatus {
		t.Fatalf("explicit error should downgrade reachability: %#v", e)
	}
	if e.AgentVersion != "9.9" || e.VolumeTotalBytes != 100 || e.VolumeUsedBytes != 40 || e.VolumeFreeBytes != 60 {
		t.Fatalf("metadata should survive a failed probe: %#v", e)
	}

	service.storeStatusResults(targets, []TargetStatusResult{
		{Index: 0, AgentVersion: "10.0", ConnectionStatus: true, VolumeTotalBytes: 200},
	})
	e, _ = service.statusCache.Get("t1")
	if e.AgentVersion != "10.0" || e.VolumeTotalBytes != 200 || e.ConnectionStatus == nil || !*e.ConnectionStatus {
		t.Fatalf("fresh probe data should overwrite: %#v", e)
	}
}

func TestAgentBatchResults(t *testing.T) {
	targets := []coredb.Target{
		{Name: "a", VolumeID: "C:"},
		{Name: "b", VolumeID: "D:"},
		{Name: "c", VolumeID: "E:"},
		{Name: "d", VolumeID: "Z:"},
	}
	resp := fswire.TargetStatusBatchResp{
		Version: "1.2.3",
		Drives: map[string]fswire.TargetDriveStatus{
			"C:": {Reachable: new(true)},
			"D:": {Reachable: new(false), Message: "not assigned"},
			"E:": {Message: "drive check timed out"},
		},
	}

	rs := agentBatchResults(targets, resp, "1.2.3")
	if rs[0].AgentVersion != "1.2.3" || !rs[0].ConnectionStatus || rs[0].Error != nil {
		t.Fatalf("reachable drive: %#v", rs[0])
	}
	if rs[1].Error == nil || rs[1].Error.Error() != "not assigned" || isTimeoutErr(rs[1].Error) {
		t.Fatalf("unreachable drive should be an explicit error: %#v", rs[1])
	}
	if !isTimeoutErr(rs[2].Error) || rs[2].ConnectionStatus {
		t.Fatalf("no-verdict drive should be timeout-class: %#v", rs[2])
	}
	if !isTimeoutErr(rs[3].Error) || rs[3].ConnectionStatus {
		t.Fatalf("missing drive should be timeout-class: %#v", rs[3])
	}
}
