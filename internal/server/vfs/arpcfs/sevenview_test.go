//go:build linux

package arpcfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
)

func loadSevenFixture(t *testing.T, name string) (*zipOverlay, []byte) {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	ov, perr := parseArchiveOverlay(func(_ context.Context, p []byte, off int64) (int, error) {
		if off >= int64(len(raw)) {
			return 0, io.EOF
		}
		n := copy(p, raw[off:])
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}, int64(len(raw)), zipMaxEntries)
	if perr != nil {
		t.Fatalf("parse %s: %v", name, perr)
	}
	return ov, raw
}

func sevenReadAt(t *testing.T, ov *zipOverlay, name string, off int64, n int) []byte {
	t.Helper()
	idx, ok := ov.byName[name]
	if !ok {
		t.Fatalf("missing entry %s", name)
	}
	zs := &zipFileState{ov: ov, ent: &ov.entries[idx], uncomp: ov.entries[idx].uncompSize}
	dest := make([]byte, n)
	m, err := zs.ReadAt(context.Background(), dest, off)
	if m != n {
		t.Fatalf("read %s@%d: got %d bytes want %d (err %v)", name, off, m, n, err)
	}
	return dest
}

func TestSevenParseAndResolve(t *testing.T) {
	ov, _ := loadSevenFixture(t, "nonsolid.7z")
	for _, name := range []string{"hello.txt", "big.txt", "bin.dat", "empty.txt", "run.sh", "dir1/dir2/deep.txt"} {
		if _, ok := ov.byName[name]; !ok {
			t.Errorf("missing file %s; byName=%v", name, ov.byName)
		}
	}
	if _, ok := ov.dirs["dir1/dir2"]; !ok {
		t.Error("missing virtual dir dir1/dir2")
	}
	if idx := ov.byName["run.sh"]; ov.entries[idx].mode&0o111 == 0 {
		t.Error("run.sh lost exec bit")
	}
	if idx := ov.byName["big.txt"]; ov.entries[idx].uncompSize != 3145728 {
		t.Errorf("big.txt size %d", ov.entries[idx].uncompSize)
	}
}

func TestSevenRead(t *testing.T) {
	for _, fx := range []string{"nonsolid.7z", "solid.7z", "store.7z"} {
		t.Run(fx, func(t *testing.T) {
			ov, _ := loadSevenFixture(t, fx)
			got := sevenReadAt(t, ov, "hello.txt", 0, 22)
			if string(got) != "hello seven zip world\n" {
				t.Errorf("hello.txt = %q", got)
			}
			got = sevenReadAt(t, ov, "dir1/dir2/deep.txt", 0, 14)
			if string(got) != "nested content" {
				t.Errorf("deep.txt = %q", got)
			}
		})
	}
}

func TestSevenRingAndRestart(t *testing.T) {
	ov, _ := loadSevenFixture(t, "solid.7z")
	tail := sevenReadAt(t, ov, "big.txt", 3145728-16, 16)
	if !bytes.Equal(tail, bytes.Repeat([]byte{'A'}, 16)) {
		t.Errorf("tail = %q", tail)
	}
	head := sevenReadAt(t, ov, "big.txt", 0, 16)
	if !bytes.Equal(head, bytes.Repeat([]byte{'A'}, 16)) {
		t.Errorf("head = %q", head)
	}
	mid := sevenReadAt(t, ov, "big.txt", 1<<20, 8)
	if !bytes.Equal(mid, bytes.Repeat([]byte{'A'}, 8)) {
		t.Errorf("mid = %q", mid)
	}
}

func TestSevenGates(t *testing.T) {
	cases := []struct {
		fixture string
		want    error
	}{
		{"many.7z", errZipTooMany},
		{"bomb.7z", errZipBomb},
		{"encrypted.7z", errZipUnsupported},
	}
	for _, tc := range cases {
		raw, err := os.ReadFile("testdata/" + tc.fixture)
		if err != nil {
			t.Fatalf("read fixture: %v", err)
		}
		ra := func(_ context.Context, p []byte, off int64) (int, error) {
			n := copy(p, raw[off:])
			if n < len(p) {
				return n, io.EOF
			}
			return n, nil
		}
		if _, err := parseArchiveOverlay(ra, int64(len(raw)), zipMaxEntries); !errors.Is(err, tc.want) {
			t.Errorf("%s: err = %v", tc.fixture, err)
		}
	}
}
