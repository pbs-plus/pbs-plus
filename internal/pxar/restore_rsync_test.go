package pxar

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	pxar "github.com/pbs-plus/pxar"
)

// containsStr reports whether ss contains s.
func containsStr(ss []string, s string) bool {
	return slices.Contains(ss, s)
}

// TestLinkTargetCandidates pins the coordinate systems a pxar hardlink
// LinkTarget may use. The restore must resolve both absolute-from-root
// (Proxmox pxar) and relative-to-self paths, normalized across OS path
// separators, since the field is not normalized across producers.
func TestLinkTargetCandidates(t *testing.T) {
	tests := []struct {
		name       string
		selfSrc    string
		linkTarget string
		want       string // a key that must be among the candidates
	}{
		{"absolute from root", "a/b/link", "/a/target", "a/target"},
		{"absolute no leading slash", "a/b/link", "a/target", "a/target"},
		{"relative to self parent", "a/b/link", "../target", "a/target"},
		{"relative sibling", "a/b/link", "target", "a/b/target"},
		{"windows backslashes normalized", `a\b\link`, `\a\target`, "a/target"},
		{"dot cleaned", "a/b/link", "./target", "a/b/target"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := linkTargetCandidates(tt.selfSrc, tt.linkTarget)
			if !containsStr(got, tt.want) {
				t.Fatalf("linkTargetCandidates(%q,%q) = %v, want to contain %q",
					tt.selfSrc, tt.linkTarget, got, tt.want)
			}
		})
	}

	if linkTargetCandidates("a", "") != nil {
		t.Fatal("empty linkTarget must yield no candidates")
	}
}

// TestHardlinkIndexRegisterAndResolve verifies a registered target is found by
// a hardlink referencing it, and that an unregistered target is not.
func TestHardlinkIndexRegisterAndResolve(t *testing.T) {
	hl := newHardlinkIndex()
	job := restoreJob{
		dest:    "/dst/link",
		srcPath: "a/link",
		info:    pxar.FileInfo{FileType: pxar.FileTypeHardlink, LinkTarget: "/a/target"},
	}

	if _, ok := hl.tryResolveNow(job); ok {
		t.Fatal("resolved before any target was registered")
	}

	hl.register("a/target", "/dst/target")
	dest, ok := hl.tryResolveNow(job)
	if !ok {
		t.Fatal("expected resolution after registering the target")
	}
	if dest != "/dst/target" {
		t.Fatalf("resolved dest = %q, want /dst/target", dest)
	}

	// register ignores empty srcPath (e.g. a root entry) without panicking.
	hl.register("", "/dst/x")
	if _, ok := hl.resolved[""]; ok {
		t.Fatal("empty srcPath should not be recorded")
	}
}

// TestHardlinkIndexDeferAndDrain verifies the deferred list is used by the
// final sweep: a link that cannot resolve inline is stashed and later drained
// in order, at which point its (now-registered) target resolves.
func TestHardlinkIndexDeferAndDrain(t *testing.T) {
	hl := newHardlinkIndex()
	j1 := restoreJob{dest: "/d/l1", srcPath: "a/l1", info: pxar.FileInfo{LinkTarget: "/a/t1"}}
	j2 := restoreJob{dest: "/d/l2", srcPath: "a/l2", info: pxar.FileInfo{LinkTarget: "/a/t2"}}

	hl.deferLink(j1)
	hl.deferLink(j2)

	hl.register("a/t1", "/d/t1")
	hl.register("a/t2", "/d/t2")

	drained := hl.drainDeferred()
	if len(drained) != 2 {
		t.Fatalf("drained %d jobs, want 2", len(drained))
	}
	// Each drained job should now resolve.
	for _, j := range drained {
		if _, ok := hl.tryResolveNow(j); !ok {
			t.Errorf("drained job %q did not resolve", j.dest)
		}
	}
	// Drain is idempotent.
	if got := hl.drainDeferred(); len(got) != 0 {
		t.Fatalf("second drain returned %d jobs, want 0", len(got))
	}
}

// TestLinkFileCreatesHardlink end-to-end checks that linkFile produces a real
// filesystem hard link sharing the target's inode.
func TestLinkFileCreatesHardlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.txt")
	link := filepath.Join(dir, "link.txt")
	if err := os.WriteFile(target, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := linkFile(target, link); err != nil {
		t.Skipf("filesystem does not support hard links: %v", err)
	}

	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	ls, err := os.Stat(link)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(st, ls) {
		t.Fatal("linkFile did not create a hard link (different inodes)")
	}
	if b, _ := os.ReadFile(link); string(b) != "payload" {
		t.Fatalf("link content = %q, want payload", b)
	}

	// linkFile replaces an existing destination (no EEXIST) and still shares
	// the inode.
	if err := os.WriteFile(link, []byte("clobbered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := linkFile(target, link); err != nil {
		t.Fatalf("linkFile over existing dest failed: %v", err)
	}
}

// TestAtomicSwapReplacesFile verifies the temp+rename swap atomically replaces
// an existing regular file and removes the temp.
func TestAtomicSwapReplacesFile(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".pxar-tmp")
	dest := filepath.Join(dir, "out")
	if err := os.WriteFile(dest, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := atomicSwap(tmp, dest); err != nil {
		t.Fatalf("atomicSwap: %v", err)
	}
	b, err := os.ReadFile(dest)
	if err != nil || string(b) != "new" {
		t.Fatalf("dest content = %q (%v), want new", b, err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp should be gone after swap, got err=%v", err)
	}
}

// TestAtomicSwapReplacesConflictingType verifies that when the destination
// exists as an incompatible type (an empty directory), the swap removes it and
// retries rather than failing or clobbering non-destructively.
func TestAtomicSwapReplacesConflictingType(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".t")
	dest := filepath.Join(dir, "out")
	if err := os.MkdirAll(dest, 0o755); err != nil { // empty dir conflicts with a file
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := atomicSwap(tmp, dest); err != nil {
		t.Fatalf("atomicSwap over conflicting type: %v", err)
	}
	if fi, err := os.Stat(dest); err != nil || !fi.Mode().IsRegular() {
		t.Fatalf("dest is not a regular file after swap: %v (%v)", fi, err)
	}
}

// TestAtomicSwapRefusesNonEmptyDir verifies atomicSwap does NOT delete a
// non-empty conflicting directory (which would lose data); it surfaces an error.
func TestAtomicSwapRefusesNonEmptyDir(t *testing.T) {
	dir := t.TempDir()
	tmp := filepath.Join(dir, ".t")
	dest := filepath.Join(dir, "out")
	if err := os.MkdirAll(filepath.Join(dest, "child"), 0o755); err != nil {
		t.Fatal(err) // non-empty dir
	}
	if err := os.WriteFile(tmp, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := atomicSwap(tmp, dest); err == nil {
		t.Fatal("expected error swapping over a non-empty directory, got nil")
	}
	// The child must still exist (no data loss).
	if _, err := os.Stat(filepath.Join(dest, "child")); err != nil {
		t.Fatalf("non-empty dir contents were destroyed: %v", err)
	}
}

// TestQuickCheckSkipsOnSizeAndMtime pins rsync's default quick-check rule
// (size + whole-second mtime): an existing file matching both is skipped
// (noAttr=false exercises the mtime branch), while a mtime mismatch forces an
// update even when sizes match.
func TestQuickCheckSkipsOnSizeAndMtime(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	const mtime int64 = 1700000000

	info := pxar.FileInfo{FileType: pxar.FileTypeFile, RawSize: 5, MtimeSecs: mtime}

	// matching size + mtime -> skip
	writeFileWithMtime(t, p, []byte("hello"), mtime)
	got, err := shouldUpdateFile(p, info, false)
	if err != nil {
		t.Fatal(err)
	}
	if got {
		t.Fatal("shouldUpdateFile=true on matching size+mtime, want skip")
	}

	// matching size, different mtime -> update
	writeFileWithMtime(t, p, []byte("hello"), mtime+60)
	got, err = shouldUpdateFile(p, info, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("shouldUpdateFile=false on mtime mismatch, want update")
	}

	// different size -> update regardless of mtime
	writeFileWithMtime(t, p, []byte("toolong"), mtime)
	got, err = shouldUpdateFile(p, info, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got {
		t.Fatal("shouldUpdateFile=false on size mismatch, want update")
	}
}

func writeFileWithMtime(t *testing.T, p string, content []byte, mtime int64) {
	t.Helper()
	if err := os.WriteFile(p, content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(p, time.Unix(mtime, 0), time.Unix(mtime, 0)); err != nil {
		t.Fatal(err)
	}
}
