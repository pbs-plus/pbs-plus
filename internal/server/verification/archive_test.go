//go:build linux

package verification

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

func TestSelectDataArchiveSkipsPluginMetadata(t *testing.T) {
	files := []string{
		proxmox.PluginMetadataArchiveName + ".mpxar.didx",
		proxmox.PluginMetadataArchiveName + ".ppxar.didx",
		"owner",
		"test-host---Root.mpxar.didx",
		"test-host---Root.ppxar.didx",
	}
	got, err := selectDataArchive(files)
	if err != nil {
		t.Fatalf("selectDataArchive: %v", err)
	}
	if got != "test-host---Root.mpxar.didx" {
		t.Fatalf("selectDataArchive = %q, want the target data archive", got)
	}

	if _, err := selectDataArchive([]string{proxmox.PluginMetadataArchiveName + ".mpxar.didx", "index.json.blob"}); err == nil {
		t.Fatal("selectDataArchive accepted a snapshot with only the plugin metadata archive")
	}
}
