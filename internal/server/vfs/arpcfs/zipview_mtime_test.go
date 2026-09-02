//go:build linux

package arpcfs

import (
	"archive/zip"
	"bytes"
	"os"
	"testing"
	"time"
)

type mtimeEntry struct {
	name     string
	modified time.Time
}

func buildZipModified(t *testing.T, entries []mtimeEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, e := range entries {
		hdr := &zip.FileHeader{Name: e.name, Method: zip.Store, Modified: e.modified}
		hdr.SetMode(0o644)
		if e.name[len(e.name)-1] == '/' {
			hdr.SetMode(0o755 | os.ModeDir)
		}
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			t.Fatal(err)
		}
		if e.name[len(e.name)-1] != '/' {
			if _, err := w.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestZipEntryMtimeHonorsExtendedTimestamp checks the 0x5455 field beats the local-time MS-DOS field.
func TestZipEntryMtimeHonorsExtendedTimestamp(t *testing.T) {
	loc := time.FixedZone("UTC+7", 7*3600)
	want := time.Date(2024, 3, 5, 14, 30, 6, 0, loc)

	ov := testOverlay(t, buildZipModified(t, []mtimeEntry{{"a.txt", want}}))
	got := ov.entryAttr("a.txt", ov.byName["a.txt"]).LastWriteTime
	if got != want.Unix() {
		t.Errorf("LastWriteTime = %d (%s), want %d (%s); off by %ds",
			got, time.Unix(got, 0).UTC(), want.Unix(), want.UTC(), got-want.Unix())
	}
}

// TestZipEntryMtimeOddSecond checks 1-second resolution survives the MS-DOS 2-second grid.
func TestZipEntryMtimeOddSecond(t *testing.T) {
	want := time.Date(2024, 3, 5, 14, 30, 7, 0, time.UTC)

	ov := testOverlay(t, buildZipModified(t, []mtimeEntry{{"odd.txt", want}}))
	got := ov.entryAttr("odd.txt", ov.byName["odd.txt"]).LastWriteTime
	if got != want.Unix() {
		t.Errorf("LastWriteTime = %d (%s), want %d (%s)",
			got, time.Unix(got, 0).UTC(), want.Unix(), want)
	}
}

// TestZipDirAttrModTime checks dirAttr populates ModTime, which Node.Lookup reads.
func TestZipDirAttrModTime(t *testing.T) {
	want := time.Date(2024, 3, 5, 14, 30, 6, 0, time.UTC)

	ov := testOverlay(t, buildZipModified(t, []mtimeEntry{
		{"d/", want},
		{"d/f.txt", want},
	}))

	d, ok := ov.dirs["d"]
	if !ok {
		t.Fatalf("missing dir d; dirs=%v", ov.dirs)
	}
	attr := ov.dirAttr(d)
	if attr.ModTime != want.Unix()*int64(time.Second) {
		t.Errorf("dir ModTime = %d, want %d", attr.ModTime, want.Unix()*int64(time.Second))
	}
	if attr.LastWriteTime != want.Unix() {
		t.Errorf("dir LastWriteTime = %d, want %d", attr.LastWriteTime, want.Unix())
	}
}

// TestZipImplicitDirMtimeBackfill covers dirs the archive never lists explicitly.
func TestZipImplicitDirMtimeBackfill(t *testing.T) {
	want := time.Date(2024, 3, 5, 14, 30, 6, 0, time.UTC)

	ov := testOverlay(t, buildZipModified(t, []mtimeEntry{{"d/e/f.txt", want}}))
	for _, name := range []string{"d", "d/e"} {
		d, ok := ov.dirs[name]
		if !ok {
			t.Fatalf("missing dir %q; dirs=%v", name, ov.dirs)
		}
		if d.mtime != want.Unix() {
			t.Errorf("dir %q mtime = %d, want %d", name, d.mtime, want.Unix())
		}
	}
}

func TestEnsureDirKeepsMtimeWithoutPerm(t *testing.T) {
	ov := &zipOverlay{byName: map[string]int32{}, dirs: map[string]*zipDir{"": {}}}
	ov.ensureParent("dir1/f.txt")
	ov.ensureDir("dir1", 0, 1788230648)

	if got := ov.dirs["dir1"].mtime; got != 1788230648 {
		t.Errorf("dir1 mtime = %d, want 1788230648", got)
	}
}

// TestSevenDirAttrModTime is TestZipDirAttrModTime against the real 7z fixture.
func TestSevenDirAttrModTime(t *testing.T) {
	ov, _ := loadSevenFixture(t, "nonsolid.7z")
	d, ok := ov.dirs["dir1"]
	if !ok {
		t.Fatalf("missing dir1; dirs=%v", ov.dirs)
	}
	if d.mtime == 0 {
		t.Fatalf("dir1 mtime not ingested")
	}
	if got := ov.dirAttr(d).ModTime; got != d.mtime*int64(time.Second) {
		t.Errorf("dir1 ModTime = %d, want %d", got, d.mtime*int64(time.Second))
	}
}
