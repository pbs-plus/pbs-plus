//go:build linux

package application

import (
	"path/filepath"
	"testing"
	"time"

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
	service.statusCache.Set("cached", StatusEntry{ConnectionStatus: true, AgentVersion: "9.9", CheckedAt: time.Now()})

	entries := service.GetStatusEntries(targets)

	if e := entries["cached"]; !e.ConnectionStatus || e.AgentVersion != "9.9" {
		t.Errorf("cached entry = %#v", e)
	}
	if e := entries["agent-no-session"]; e.ConnectionStatus || !e.CheckedAt.IsZero() || e.AgentVersion != "N/A" {
		t.Errorf("agent without session or cache = %#v", e)
	}
	if e := entries["local-ok"]; !e.ConnectionStatus || e.VolumeTotalBytes <= 0 || e.CheckedAt.IsZero() {
		t.Errorf("local ok entry = %#v", e)
	}
	if e := entries["local-missing"]; e.ConnectionStatus || e.LastError == "" {
		t.Errorf("local missing entry = %#v", e)
	}
	if e := entries["s3-no-cache"]; e.ConnectionStatus || !e.CheckedAt.IsZero() {
		t.Errorf("never-probed s3 entry = %#v", e)
	}
}

func TestStatusShardWraps(t *testing.T) {
	targets := make([]coredb.Target, 10)
	for i := range targets {
		targets[i] = coredb.Target{Name: string(rune('0' + i))}
	}
	names := func(batch []coredb.Target) string {
		s := ""
		for _, b := range batch {
			s += b.Name
		}
		return s
	}
	if got := names(statusShard(targets, 8, 4)); got != "8901" {
		t.Errorf("wrapped shard = %q", got)
	}
	if got := statusShard(targets, 0, 10); len(got) != 10 {
		t.Errorf("full shard len = %d", len(got))
	}
	if got := statusShard(targets, 0, 12); len(got) != 10 {
		t.Errorf("oversized shard len = %d", len(got))
	}
}

func TestStatusSweeperFillsAndEvicts(t *testing.T) {
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

	service.sweepStatuses(t.Context(), 4)

	ok, found := service.statusCache.Get("local-ok")
	if !found || !ok.ConnectionStatus || ok.CheckedAt.IsZero() {
		t.Errorf("local-ok entry = %#v found=%v", ok, found)
	}
	missing, found := service.statusCache.Get("local-missing")
	if !found || missing.ConnectionStatus || missing.LastError == "" {
		t.Errorf("local-missing entry = %#v found=%v", missing, found)
	}

	if err := service.DeleteTarget(nil, "local-ok"); err != nil {
		t.Fatal(err)
	}
	remaining, err := service.GetAllTargets()
	if err != nil {
		t.Fatal(err)
	}
	service.evictMissingTargets(remaining)
	if _, found := service.statusCache.Get("local-ok"); found {
		t.Error("evicted target still cached")
	}
	if _, found := service.statusCache.Get("local-missing"); !found {
		t.Error("live target wrongly evicted")
	}
}
