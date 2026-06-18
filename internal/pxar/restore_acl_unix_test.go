//go:build unix

package pxar

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

// TestPackACLDropsUnknownTags verifies the defensive guard inside
// applyUnixACLsFd: entries with empty or unrecognized tags are dropped so a
// (legacy/partial, or wrongly-typed) payload can never emit a tag-0 ACL entry
// that would corrupt the on-disk POSIX ACL.
func TestPackACLDropsUnknownTags(t *testing.T) {
	known := map[string]struct{}{
		"user_obj": {}, "user": {}, "group_obj": {},
		"group": {}, "mask": {}, "other": {},
	}

	entries := []types.PosixACL{
		{Tag: "user_obj", Perms: 7},
		{Tag: "", Perms: 5},      // empty → dropped
		{Tag: "bogus", Perms: 1}, // unknown → dropped
		{Tag: "other", Perms: 4},
	}

	var kept []types.PosixACL
	for _, ent := range entries {
		if _, ok := known[ent.Tag]; !ok {
			continue
		}
		kept = append(kept, ent)
	}
	if len(kept) != 2 {
		t.Fatalf("expected 2 known-tag entries kept, got %d", len(kept))
	}

	packed := packACL(kept)
	// 4-byte header + 8 bytes per entry.
	if want := 4 + 8*len(kept); len(packed) != want {
		t.Fatalf("packed len = %d, want %d", len(packed), want)
	}
}
