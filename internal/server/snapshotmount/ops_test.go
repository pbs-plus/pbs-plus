//go:build linux

package snapshotmount

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func TestParseMountPoints(t *testing.T) {
	input := `24 1 0:20 / / rw - overlay overlay rw
25 24 0:21 / /mnt/Project\040A/D-100 ro - fuse.pxar pxar ro
26 24 0:22 / /mnt/with\011tab\012newline\134slash ro - fuse.pxar pxar ro
27 24 0:23 / /mnt/literal\134040 ro - fuse.pxar pxar ro
malformed
28 24 0:24 / /mnt/missing-separator ro
`
	got, err := parseMountPoints(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"/", "/mnt/Project A/D-100", "/mnt/with\ttab\nnewline\\slash", "/mnt/literal\\040"}
	if !slices.Equal(got, want) {
		t.Fatalf("mount points = %q, want %q", got, want)
	}
}

func TestParseMountPointsReadError(t *testing.T) {
	want := errors.New("mountinfo read failed")
	if _, err := parseMountPoints(iotest.ErrReader(want)); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestIsMountedSymlink(t *testing.T) {
	alias := filepath.Join(t.TempDir(), "Project A")
	if err := os.Symlink("/proc", alias); err != nil {
		t.Fatal(err)
	}
	if !IsMounted(alias) {
		t.Fatal("symlink to /proc not recognized as mounted")
	}
	if IsMounted(t.TempDir()) {
		t.Fatal("ordinary directory recognized as mounted")
	}
}
