//go:build linux

package targetapi

import (
	"encoding/json"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestTargetResponsePreservesLegacyType(t *testing.T) {
	encoded, err := json.Marshal(newTargetResponse(coredb.Target{
		Name:   "remote-root",
		Type:   coredb.TargetTypeFilesystem,
		Access: coredb.FilesystemAccessAgent,
	}))
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if got["target_type"] != "agent" || got["kind"] != "filesystem" || got["access"] != "agent" {
		t.Fatalf("target response = %s", encoded)
	}
}
