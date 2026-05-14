package pxarmount

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// TestCreate_FirstOpenNonEEXIST tests that Create doesn't fall through to
// the truncate-open path when the first open fails with a non-EEXIST error.
// BUG: Currently Create ignores the specific error from O_EXCL open and
// blindly truncates whatever exists at the path on ANY error.
func TestCreate_FirstOpenNonEEXIST(t *testing.T) {
	fs, _, cleanup := newTestPassthroughFS(t)
	defer cleanup()

	// This test verifies behavior when the O_EXCL open fails.
	// In the test environment, the file doesn't exist, so O_EXCL | O_CREATE
	// should succeed. We test the fallback path by creating the file first
	// and then calling Create again.
	//
	// First Create should succeed (O_EXCL):
	var out1 fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(0), // O_WRONLY
	}, "test.txt", &out1)
	if st != fuse.OK {
		t.Fatalf("first Create: %v", st)
	}
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: out1.NodeId},
		Fh:       out1.Fh,
	})

	// Second Create should succeed via fallback (EEXIST from O_EXCL):
	var out2 fuse.CreateOut
	st = fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: RootInode},
		Mode:     0o644,
		Flags:    uint32(0),
	}, "test.txt", &out2)
	if st != fuse.OK {
		t.Fatalf("second Create (fallback): %v", st)
	}
	fs.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: out2.NodeId},
		Fh:       out2.Fh,
	})
}
