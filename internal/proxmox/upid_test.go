package proxmox

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildPxarPathsSkipsPluginMetadataArchive(t *testing.T) {
	root := t.TempDir()
	groupDir := filepath.Join(root, "host", "job-1", "2026-01-01T00:00:00Z")
	if err := os.MkdirAll(groupDir, 0o700); err != nil {
		t.Fatalf("create group dir: %v", err)
	}
	for _, name := range []string{
		PluginMetadataArchiveName + ".mpxar.didx",
		PluginMetadataArchiveName + ".ppxar.didx",
		"zdata.mpxar.didx",
		"zdata.ppxar.didx",
	} {
		if err := os.WriteFile(filepath.Join(groupDir, name), nil, 0o600); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}

	mpxarPath, ppxarPath, split, err := BuildPxarPaths(root, "", "host", "job-1", "2026-01-01T00:00:00Z", "")
	if err != nil || !split {
		t.Fatalf("BuildPxarPaths = %v, split %v", err, split)
	}
	if filepath.Base(mpxarPath) != "zdata.mpxar.didx" || filepath.Base(ppxarPath) != "zdata.ppxar.didx" {
		t.Fatalf("selected %s and %s, want the data archive", mpxarPath, ppxarPath)
	}

	mpxarPath, _, _, err = BuildPxarPaths(root, "", "host", "job-1", "2026-01-01T00:00:00Z", PluginMetadataArchiveName+".mpxar.didx")
	if err != nil || filepath.Base(mpxarPath) != PluginMetadataArchiveName+".mpxar.didx" {
		t.Fatalf("explicit metadata archive = %s, %v", mpxarPath, err)
	}
}
