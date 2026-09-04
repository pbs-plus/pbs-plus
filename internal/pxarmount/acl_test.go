package pxarmount

import "testing"

func TestForcedZeroOwnership(t *testing.T) {
	entry := ResolvedEntry{UID: 1000, GID: 1000}
	fs := MutableFS{acl: BuildACLConfig(0, 0, true, true, "", "")}
	fs.applyACL(&entry)
	if entry.UID != 0 || entry.GID != 0 {
		t.Fatalf("ownership = %d:%d, want 0:0", entry.UID, entry.GID)
	}

	entry = ResolvedEntry{UID: 1000, GID: 1000}
	fs.acl = BuildACLConfig(0, 0, false, false, "", "")
	fs.applyACL(&entry)
	if entry.UID != 1000 || entry.GID != 1000 {
		t.Fatalf("inherited ownership = %d:%d, want 1000:1000", entry.UID, entry.GID)
	}
}
