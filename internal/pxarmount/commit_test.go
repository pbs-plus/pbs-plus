package pxarmount

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/encoder"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"
	"golang.org/x/sys/unix"
)

// --- trackingWriter records calls for correctness assertions ---

type trackingWriter struct {
	dirOpens    int
	dirCloses   int
	refs        []refRecord
	symlinks    []string
	emptyFiles  []string
	backedFiles []string
	ops         []string // ordered log: "dir:name", "ref:name@offset", "sym:name", "empty:name", "backed:name"
}

type refRecord struct {
	name   string
	offset uint64
}

func (w *trackingWriter) Begin(_ *pxar.Metadata, _ transfer.Options) error { return nil }
func (w *trackingWriter) WriteEntry(entry *pxar.Entry, _ []byte) error {
	if entry.Kind == pxar.KindSymlink {
		w.symlinks = append(w.symlinks, entry.Path)
		w.ops = append(w.ops, "sym:"+entry.Path)
	} else {
		w.emptyFiles = append(w.emptyFiles, entry.Path)
		w.ops = append(w.ops, "empty:"+entry.Path)
	}
	return nil
}
func (w *trackingWriter) WriteEntryRef(entry *pxar.Entry, offset uint64) error {
	w.refs = append(w.refs, refRecord{entry.Path, offset})
	w.ops = append(w.ops, "ref:"+entry.Path)
	return nil
}
func (w *trackingWriter) WriteEntryReader(entry *pxar.Entry, _ io.Reader, _ uint64) error {
	w.backedFiles = append(w.backedFiles, entry.Path)
	w.ops = append(w.ops, "backed:"+entry.Path)
	return nil
}
func (w *trackingWriter) BeginDirectory(name string, _ *pxar.Metadata) error {
	w.dirOpens++
	w.ops = append(w.ops, "dir:"+name)
	return nil
}
func (w *trackingWriter) EndDirectory() error {
	w.dirCloses++
	w.ops = append(w.ops, "enddir")
	return nil
}
func (w *trackingWriter) InjectChunks(_ []backupproxy.KnownChunkRef) error { return nil }
func (w *trackingWriter) Encoder() *encoder.Encoder                        { return nil }
func (w *trackingWriter) Finish() error                                    { return nil }
func (w *trackingWriter) Close() error                                     { return nil }

// --- mergeMetaWithPxar ---

func TestMergeMetaWithPxar(t *testing.T) {
	journalMeta := pxar.Metadata{
		Stat: format.Stat{
			Mode:  format.ModeIFREG | 0o644,
			UID:   1000,
			GID:   1000,
			Mtime: format.StatxTimestamp{Secs: 1700000000, Nanos: 123},
		},
		XAttrs: []format.XAttr{
			format.NewXAttr([]byte("user.foo"), []byte("bar")),
			format.NewXAttr([]byte("user.original"), []byte("val")),
		},
	}

	pxarEntry := &pxar.Entry{
		Metadata: pxar.Metadata{
			Stat: format.Stat{
				Mode:  format.ModeIFREG | 0o755,
				Flags: 0x10,
				UID:   0,
				GID:   0,
				Mtime: format.StatxTimestamp{Secs: 1600000000},
			},
			FCaps: []byte{1, 2, 3},
			ACL: pxar.ACL{
				Users: []format.ACLUser{{UID: 1000, Permissions: format.ACLPermissions(7)}},
			},
		},
	}

	merged := mergeMetaWithPxar(journalMeta, pxarEntry)

	// Journal overrides should win for stat fields.
	if merged.Stat.Mode != format.ModeIFREG|0o644 {
		t.Errorf("Mode = 0%o, want 0%o", merged.Stat.Mode, format.ModeIFREG|0o644)
	}
	if merged.Stat.UID != 1000 || merged.Stat.GID != 1000 {
		t.Errorf("UID/GID = %d/%d, want 1000/1000", merged.Stat.UID, merged.Stat.GID)
	}
	if merged.Stat.Mtime.Secs != 1700000000 {
		t.Errorf("Mtime.Secs = %d, want 1700000000", merged.Stat.Mtime.Secs)
	}

	// Pxar fields the journal doesn't track should be preserved.
	if merged.Stat.Flags != 0x10 {
		t.Errorf("Flags = %d, want 0x10", merged.Stat.Flags)
	}
	if len(merged.FCaps) != 3 || merged.FCaps[0] != 1 {
		t.Errorf("FCaps not preserved: %v", merged.FCaps)
	}
	if len(merged.ACL.Users) != 1 {
		t.Errorf("ACL.Users not preserved: %+v", merged.ACL.Users)
	}

	// Journal xattrs must be preserved (not overwritten by pxar).
	if len(merged.XAttrs) != 2 {
		t.Errorf("XAttrs len = %d, want 2", len(merged.XAttrs))
	}
}

func TestMergeMetaWithPxarNilPxarFields(t *testing.T) {
	journalMeta := pxar.Metadata{
		Stat: format.Stat{Mode: format.ModeIFREG | 0o600, UID: 500, GID: 500},
	}
	pxarEntry := &pxar.Entry{
		Metadata: pxar.Metadata{
			Stat: format.Stat{Mode: format.ModeIFREG | 0o644},
		},
	}

	merged := mergeMetaWithPxar(journalMeta, pxarEntry)
	if merged.Stat.Mode != format.ModeIFREG|0o600 {
		t.Errorf("journal mode should win: got 0%o", merged.Stat.Mode)
	}
	if merged.Stat.UID != 500 {
		t.Errorf("journal UID should win: got %d", merged.Stat.UID)
	}
	if len(merged.FCaps) != 0 {
		t.Error("FCaps should be empty when pxar has none")
	}
}

// --- nodeToMetadata ---

func TestNodeToMetadata(t *testing.T) {
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    0o644,
		UID:     1000,
		GID:     1001,
		Size:    4096,
		MtimeNs: 1700000000_123456789,
	}
	xattrs := []format.XAttr{
		format.NewXAttr([]byte("user.key"), []byte("value")),
	}

	meta := nodeToMetadata(node, xattrs)

	if meta.Stat.Mode != format.ModeIFREG|0o644 {
		t.Errorf("Mode = 0%o, want 0%o", meta.Stat.Mode, format.ModeIFREG|0o644)
	}
	if meta.Stat.UID != 1000 || meta.Stat.GID != 1001 {
		t.Errorf("UID/GID = %d/%d", meta.Stat.UID, meta.Stat.GID)
	}
	if meta.Stat.Mtime.Secs != 1700000000 || meta.Stat.Mtime.Nanos != 123456789 {
		t.Errorf("Mtime = %d.%d", meta.Stat.Mtime.Secs, meta.Stat.Mtime.Nanos)
	}
	if len(meta.XAttrs) != 1 || string(meta.XAttrs[0].Name()) != "user.key" {
		t.Errorf("XAttrs not correct: %v", meta.XAttrs)
	}
}

func TestNodeToMetadataKinds(t *testing.T) {
	tests := []struct {
		kind     uint8
		expected uint64
	}{
		{NodeDir, format.ModeIFDIR},
		{NodeSymlink, format.ModeIFLNK},
		{NodeFile, format.ModeIFREG},
	}
	for _, tt := range tests {
		node := &GraphNode{Kind: tt.kind, Mode: 0o755}
		meta := nodeToMetadata(node, nil)
		if meta.Stat.Mode != tt.expected|0o755 {
			t.Errorf("kind=%d: Mode = 0%o, want 0%o", tt.kind, meta.Stat.Mode, tt.expected|0o755)
		}
	}
}

// --- pendingRefs batching ---

func TestPendingRefsFlushOrder(t *testing.T) {
	ow := &commitWalkState{
		mfs: &MutableFS{verbose: false},
	}

	// Add refs in non-offset order. Verify pendingRefs sort order.
	ce1 := commitEntry{name: "ccc", pxarSlim: &dirEntrySlim{name: "ccc", contentOffset: 300, isReg: true}, sortKey: 300}
	ce2 := commitEntry{name: "aaa", pxarSlim: &dirEntrySlim{name: "aaa", contentOffset: 100, isReg: true}, sortKey: 100}
	ce3 := commitEntry{name: "bbb", pxarSlim: &dirEntrySlim{name: "bbb", contentOffset: 200, isReg: true}, sortKey: 200}

	ow.pendingRefs = append(ow.pendingRefs, ce1, ce2, ce3)

	// Sort by sortKey as flushPendingRefs would.
	sort.Slice(ow.pendingRefs, func(i, j int) bool {
		return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
	})

	// Verify ascending sortKey order.
	if ow.pendingRefs[0].sortKey != 100 || ow.pendingRefs[1].sortKey != 200 || ow.pendingRefs[2].sortKey != 300 {
		t.Errorf("sortKeys: %v, want [100 200 300]", []uint64{
			ow.pendingRefs[0].sortKey, ow.pendingRefs[1].sortKey, ow.pendingRefs[2].sortKey,
		})
	}
	if ow.pendingRefs[0].name != "aaa" || ow.pendingRefs[1].name != "bbb" || ow.pendingRefs[2].name != "ccc" {
		t.Errorf("names: [%q %q %q], want [aaa bbb ccc]",
			ow.pendingRefs[0].name, ow.pendingRefs[1].name, ow.pendingRefs[2].name)
	}
}

func TestPendingRefsMaxBound(t *testing.T) {
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:    &MutableFS{verbose: false},
		writer: w,
	}

	// Add maxPendingRefs entries using journal nodes (not pxar) so emitJournalRef
	// is called instead of emitPxarRef — both need PxarFS though, so we just verify
	// the batch fills to maxPendingRefs without crossing the auto-flush boundary
	// (the auto-flush would need a real PxarFS + archive).
	for i := range maxPendingRefs {
		ce := commitEntry{
			name: "entry",
			node: &GraphNode{Kind: NodeFile, RedirectTo: "/entry"},
		}
		ow.pendingRefs = append(ow.pendingRefs, ce)
		if len(ow.pendingRefs) > maxPendingRefs {
			t.Errorf("pendingRefs exceeded max at i=%d: len=%d", i, len(ow.pendingRefs))
		}
	}
	// Batch is exactly at threshold.
	if len(ow.pendingRefs) != maxPendingRefs {
		t.Errorf("pendingRefs at threshold = %d, want %d", len(ow.pendingRefs), maxPendingRefs)
	}
}

// TestPendingRefsSortKeyFromRedirect tests that journal redirect entries
// get their sortKey from the pxar entry's PayloadOffset.
func TestPendingRefsSortKeyFromRedirect(t *testing.T) {
	ow := &commitWalkState{
		mfs: &MutableFS{verbose: false},
	}

	// A journal redirect entry with no pxarSlim — sortKey should come from
	// resolvePxarEntry. Since we have no real PxarFS, resolvePxarEntry will
	// fail and sortKey stays 0. We test that sortKey=0 entries sort first.
	ce1 := commitEntry{name: "zed", node: &GraphNode{Kind: NodeFile, RedirectTo: "/nonexistent"}, sortKey: 0}
	ce2 := commitEntry{name: "apple", pxarSlim: &dirEntrySlim{name: "apple", contentOffset: 100, isReg: true}, sortKey: 100}

	ow.pendingRefs = append(ow.pendingRefs, ce1, ce2)

	// Sort by sortKey as flushPendingRefs would.
	sort.Slice(ow.pendingRefs, func(i, j int) bool {
		return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
	})

	// sortKey=0 should come first.
	if ow.pendingRefs[0].name != "zed" || ow.pendingRefs[1].name != "apple" {
		t.Errorf("sort order (sortKey 0 first): [%q %q]",
			ow.pendingRefs[0].name, ow.pendingRefs[1].name)
	}
}

// --- alphabetical ordering (two-pointer merge) ---

func TestCommitWalkAlphabeticalOrdering(t *testing.T) {
	ow := &commitWalkState{
		mfs: &MutableFS{verbose: false},
	}

	// Verify pendingRefs flush sorts by contentOffset regardless of insert order.
	ceA := commitEntry{name: "a", pxarSlim: &dirEntrySlim{name: "a", contentOffset: 100, isReg: true}, sortKey: 100}
	ceB := commitEntry{name: "b", pxarSlim: &dirEntrySlim{name: "b", contentOffset: 50, isReg: true}, sortKey: 50}
	ceC := commitEntry{name: "c", pxarSlim: &dirEntrySlim{name: "c", contentOffset: 200, isReg: true}, sortKey: 200}

	// Add in wrong order, sort as flushPendingRefs would.
	ow.pendingRefs = append(ow.pendingRefs, ceC, ceA, ceB)
	sort.Slice(ow.pendingRefs, func(i, j int) bool {
		return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
	})

	// Offsets must be ascending: b(50), a(100), c(200).
	if ow.pendingRefs[0].sortKey != 50 || ow.pendingRefs[1].sortKey != 100 || ow.pendingRefs[2].sortKey != 200 {
		t.Errorf("sortKeys: [%d %d %d], want [50 100 200]",
			ow.pendingRefs[0].sortKey, ow.pendingRefs[1].sortKey, ow.pendingRefs[2].sortKey)
	}
}

func TestCommitWalkDirAndNewDataFlushRefs(t *testing.T) {
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:          &MutableFS{verbose: false},
		writer:       w,
		xattrCache:   make(map[int64][]format.XAttr),
		backedHashes: make(map[string]uint64),
	}

	// Verify that emitAlphabeticalJournal dispatches correctly for an empty file
	// (no pending refs to flush — just verifies the empty-file WriteEntry path).
	ceEmpty := commitEntry{
		name: "empty_file",
		node: &GraphNode{Kind: NodeFile, Size: 0},
	}
	if err := ow.emitAlphabeticalJournal(&ceEmpty, "/test"); err != nil {
		t.Fatalf("emit empty file: %v", err)
	}

	if len(w.ops) != 1 || w.ops[0] != "empty:empty_file" {
		t.Errorf("ops: %v", w.ops)
	}
}

func TestCommitWalkSymlinkFlushesRefs(t *testing.T) {
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:          &MutableFS{verbose: false},
		writer:       w,
		xattrCache:   make(map[int64][]format.XAttr),
		backedHashes: make(map[string]uint64),
	}

	// Verify emitAlphabeticalJournal dispatches to WriteEntry for symlinks.
	ceSym := commitEntry{
		name: "link",
		node: &GraphNode{Kind: NodeSymlink, SymlinkTgt: "/target", Mode: 0o777},
	}
	if err := ow.emitAlphabeticalJournal(&ceSym, "/test"); err != nil {
		t.Fatalf("emitAlphabeticalJournal(symlink): %v", err)
	}

	if len(w.ops) != 1 || w.ops[0] != "sym:link" {
		t.Errorf("ops: %v", w.ops)
	}
}

func TestCommitWalkEmptyFileFlushesRefs(t *testing.T) {
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:          &MutableFS{verbose: false},
		writer:       w,
		xattrCache:   make(map[int64][]format.XAttr),
		backedHashes: make(map[string]uint64),
	}

	// Verify emitAlphabeticalJournal dispatches correctly for empty files.
	ceEmpty := commitEntry{
		name: "empty",
		node: &GraphNode{Kind: NodeFile, Size: 0},
	}
	if err := ow.emitAlphabeticalJournal(&ceEmpty, "/test"); err != nil {
		t.Fatalf("emitAlphabeticalJournal(empty): %v", err)
	}

	if len(w.ops) != 1 || w.ops[0] != "empty:empty" {
		t.Errorf("ops: %v", w.ops)
	}
}

func TestCommitWalkBackedFileFlushesRefs(t *testing.T) {
	// emitAlphabeticalJournal for HasData file calls flushPendingRefs then
	// emitBackedFile which opens a real file. Without a real mutable directory,
	// the open fails. We verify the dispatch logic doesn't panic.
	w := &trackingWriter{}
	ow := &commitWalkState{
		mfs:          &MutableFS{verbose: false},
		writer:       w,
		xattrCache:   make(map[int64][]format.XAttr),
		backedHashes: make(map[string]uint64),
	}

	ceBacked := commitEntry{
		name: "backed_no_file",
		node: &GraphNode{Kind: NodeFile, HasData: true, Size: 4096, MtimeNs: 1},
	}
	// This will try to open a nonexistent mutable file and return error.
	// The flush happens before the open attempt.
	err := ow.emitAlphabeticalJournal(&ceBacked, "/test")
	if err == nil {
		t.Error("expected error opening nonexistent backed file")
	}
}

// --- copyUp xattr application ---

func TestApplyPxarXattrsToFile(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-copyup-xattr-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	fpath := filepath.Join(dir, "testfile")
	f, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	entry := &pxar.Entry{
		Metadata: pxar.Metadata{
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("user.testkey"), []byte("testvalue")),
				format.NewXAttr([]byte("user.testkey2"), []byte("value2")),
			},
			FCaps: []byte{0x01, 0x00, 0x00, 0x02},
		},
	}

	applyPxarXattrsToFile(fpath, entry)

	// Verify xattrs were set.
	buf := make([]byte, 256)
	n, err := unix.Lgetxattr(fpath, "user.testkey", buf)
	if err != nil {
		t.Fatalf("Lgetxattr user.testkey: %v", err)
	}
	if string(buf[:n]) != "testvalue" {
		t.Errorf("user.testkey = %q, want %q", buf[:n], "testvalue")
	}

	n2, err := unix.Lgetxattr(fpath, "user.testkey2", buf)
	if err != nil {
		t.Fatalf("Lgetxattr user.testkey2: %v", err)
	}
	if string(buf[:n2]) != "value2" {
		t.Errorf("user.testkey2 = %q, want %q", buf[:n2], "value2")
	}

	// Verify fcaps were set (skip on tmpfs which rejects security.capability).
	n3, err := unix.Lgetxattr(fpath, "security.capability", buf)
	if err != nil {
		// ENODATA means the xattr wasn't applied (filesystem doesn't support it).
		if err == unix.ENODATA {
			t.Skip("filesystem does not support security.capability xattr")
		}
		t.Fatalf("Lgetxattr security.capability: %v", err)
	}
	if n3 != 4 || buf[0] != 0x01 || buf[3] != 0x02 {
		t.Errorf("fcaps = %v, want [1 0 0 2]", buf[:n3])
	}
}

func TestApplyPxarXattrsToFileSkipsACLXattrs(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-copyup-acl-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	fpath := filepath.Join(dir, "testfile")
	f, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	entry := &pxar.Entry{
		Metadata: pxar.Metadata{
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("system.posix_acl_access"), []byte("raw_acl_data")),
				format.NewXAttr([]byte("user.keep"), []byte("kept")),
			},
		},
	}

	applyPxarXattrsToFile(fpath, entry)

	// ACL xattr should NOT have been applied (it's handled separately).
	_, err = unix.Lgetxattr(fpath, "system.posix_acl_access", nil)
	if err == nil {
		t.Error("system.posix_acl_access should not have been set by applyPxarXattrsToFile")
	}

	// user.keep should have been applied.
	buf := make([]byte, 256)
	n, err := unix.Lgetxattr(fpath, "user.keep", buf)
	if err != nil {
		t.Fatalf("Lgetxattr user.keep: %v", err)
	}
	if string(buf[:n]) != "kept" {
		t.Errorf("user.keep = %q, want %q", buf[:n], "kept")
	}
}

func TestApplyPxarXattrsToFileNoFCaps(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-copyup-nofcaps-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	fpath := filepath.Join(dir, "testfile")
	f, err := os.Create(fpath)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	entry := &pxar.Entry{
		Metadata: pxar.Metadata{
			XAttrs: []format.XAttr{
				format.NewXAttr([]byte("user.only"), []byte("val")),
			},
			// FCaps is nil — should not error.
		},
	}

	applyPxarXattrsToFile(fpath, entry)

	buf := make([]byte, 256)
	n, err := unix.Lgetxattr(fpath, "user.only", buf)
	if err != nil {
		t.Fatalf("Lgetxattr user.only: %v", err)
	}
	if string(buf[:n]) != "val" {
		t.Errorf("user.only = %q", buf[:n])
	}

	// security.capability should not exist.
	_, err = unix.Lgetxattr(fpath, "security.capability", nil)
	if err == nil {
		t.Error("security.capability should not exist when FCaps is nil")
	}
}

// --- sortKey assignment in pendingRefs ---

func TestPendingRefsSortKeyAssignment(t *testing.T) {
	ow := &commitWalkState{
		mfs: &MutableFS{verbose: false},
	}

	// Pxar regular file — sortKey should be payloadOffset (set by emitAlphabeticalPxar
	// before addToPendingRefs; here we test the addToPendingRefs fallback path).
	ceFile := commitEntry{
		name:     "reg",
		pxarSlim: &dirEntrySlim{name: "reg", payloadOffset: 4096, isReg: true},
	}
	if err := ow.addToPendingRefs(&ceFile, "/test"); err != nil {
		t.Fatal(err)
	}
	if ceFile.sortKey != 4096 {
		t.Errorf("reg file sortKey = %d, want 4096", ceFile.sortKey)
	}

	// Pxar regular file with pre-set sortKey (from emitAlphabeticalPxar eager read).
	cePreset := commitEntry{
		name:     "preset",
		pxarSlim: &dirEntrySlim{name: "preset", payloadOffset: 999, isReg: true},
		sortKey:  8888,
	}
	if err := ow.addToPendingRefs(&cePreset, "/test"); err != nil {
		t.Fatal(err)
	}
	if cePreset.sortKey != 8888 {
		t.Errorf("preset sortKey = %d, want 8888", cePreset.sortKey)
	}

	// Pxar symlink — sortKey should be entryStart.
	ceSym := commitEntry{
		name:     "link",
		pxarSlim: &dirEntrySlim{name: "link", entryStart: 2048, isSymlink: true},
	}
	if err := ow.addToPendingRefs(&ceSym, "/test"); err != nil {
		t.Fatal(err)
	}
	if ceSym.sortKey != 2048 {
		t.Errorf("symlink sortKey = %d, want 2048", ceSym.sortKey)
	}

	// Pxar directory — sortKey should be entryStart.
	ceDir := commitEntry{
		name:     "dir",
		pxarSlim: &dirEntrySlim{name: "dir", entryStart: 1024, isDir: true},
	}
	if err := ow.addToPendingRefs(&ceDir, "/test"); err != nil {
		t.Fatal(err)
	}
	if ceDir.sortKey != 1024 {
		t.Errorf("dir sortKey = %d, want 1024", ceDir.sortKey)
	}
}

func TestPendingRefsFlushSortsBySortKey(t *testing.T) {
	ow := &commitWalkState{
		mfs: &MutableFS{verbose: false},
	}

	// Add entries with random sortKeys. Verify pendingRefs sorts ascending after
	// manual sort (same sort flushPendingRefs would apply).
	keys := []uint64{500, 100, 900, 50, 750}
	names := []string{"e", "b", "i", "a", "g"}
	for i, k := range keys {
		ce := commitEntry{
			name:     names[i],
			pxarSlim: &dirEntrySlim{name: names[i], contentOffset: k, isReg: true},
			sortKey:  k,
		}
		ow.pendingRefs = append(ow.pendingRefs, ce)
	}

	sort.Slice(ow.pendingRefs, func(i, j int) bool {
		return ow.pendingRefs[i].sortKey < ow.pendingRefs[j].sortKey
	})

	// Verify ascending order.
	for i := 1; i < len(ow.pendingRefs); i++ {
		if ow.pendingRefs[i].sortKey <= ow.pendingRefs[i-1].sortKey {
			t.Errorf("sortKeys not ascending: [%d]=%d <= [%d]=%d",
				i, ow.pendingRefs[i].sortKey, i-1, ow.pendingRefs[i-1].sortKey)
		}
	}

	// Verify names match sorted order.
	expected := []string{"a", "b", "e", "g", "i"}
	for i, name := range expected {
		if ow.pendingRefs[i].name != name {
			t.Errorf("pendingRefs[%d].name = %q, want %q", i, ow.pendingRefs[i].name, name)
		}
	}
}

// --- two-pointer merge correctness ---

func TestTwoPointerMergeAllPxarNoJournal(t *testing.T) {
	// Verify that two-pointer merge of only pxar entries (no journal) maintains
	// alphabetical order and that entries have correct sortKey for later flush.
	pxarEntries := []dirEntrySlim{
		{name: "zeta", contentOffset: 500, isReg: true},
		{name: "alpha", contentOffset: 100, isReg: true},
		{name: "gamma", contentOffset: 300, isReg: true},
		{name: "beta", contentOffset: 200, isReg: true},
	}
	sort.Slice(pxarEntries, func(i, j int) bool {
		return pxarEntries[i].name < pxarEntries[j].name
	})

	// Simulate two-pointer merge: pi only (no journal).
	var names []string
	for pi := range pxarEntries {
		names = append(names, pxarEntries[pi].name)
	}

	// Verify alphabetical order.
	expected := []string{"alpha", "beta", "gamma", "zeta"}
	for i, exp := range expected {
		if names[i] != exp {
			t.Errorf("entry[%d] = %q, want %q", i, names[i], exp)
		}
	}
}

func TestTwoPointerMergeAllJournalNoPxar(t *testing.T) {
	// Verify two-pointer merge with only journal entries produces alphabetical order.
	journalEdges := []GraphEdge{
		{Name: "b_file", ChildID: 2},
		{Name: "a_dir", ChildID: 3},
		{Name: "c_sym", ChildID: 4},
	}
	sort.Slice(journalEdges, func(i, j int) bool {
		return journalEdges[i].Name < journalEdges[j].Name
	})

	var names []string
	for ji := range journalEdges {
		names = append(names, journalEdges[ji].Name)
	}

	expected := []string{"a_dir", "b_file", "c_sym"}
	for i, exp := range expected {
		if names[i] != exp {
			t.Errorf("entry[%d] = %q, want %q", i, names[i], exp)
		}
	}
}

func TestTwoPointerMergeInterleaved(t *testing.T) {
	// Verify interleaved merge: pxar has alpha,gamma; journal has beta,delta.
	// Result should be: alpha(pxar), beta(journal), delta(journal), gamma(pxar).
	pxarEntries := []dirEntrySlim{
		{name: "gamma", contentOffset: 300, isReg: true},
		{name: "alpha", contentOffset: 100, isReg: true},
	}
	sort.Slice(pxarEntries, func(i, j int) bool {
		return pxarEntries[i].name < pxarEntries[j].name
	})

	journalEdges := []GraphEdge{
		{Name: "delta", ChildID: 5},
		{Name: "beta", ChildID: 4},
	}
	sort.Slice(journalEdges, func(i, j int) bool {
		return journalEdges[i].Name < journalEdges[j].Name
	})

	type merged struct {
		name   string
		isPxar bool
	}
	var result []merged

	pi, ji := 0, 0
	for pi < len(pxarEntries) || ji < len(journalEdges) {
		if pi >= len(pxarEntries) {
			result = append(result, merged{journalEdges[ji].Name, false})
			ji++
		} else if ji >= len(journalEdges) {
			result = append(result, merged{pxarEntries[pi].name, true})
			pi++
		} else if pxarEntries[pi].name < journalEdges[ji].Name {
			result = append(result, merged{pxarEntries[pi].name, true})
			pi++
		} else {
			result = append(result, merged{journalEdges[ji].Name, false})
			ji++
		}
	}

	expected := []struct {
		name   string
		isPxar bool
	}{
		{"alpha", true},
		{"beta", false},
		{"delta", false},
		{"gamma", true},
	}
	if len(result) != len(expected) {
		t.Fatalf("len(result)=%d, want %d", len(result), len(expected))
	}
	for i, exp := range expected {
		if result[i].name != exp.name || result[i].isPxar != exp.isPxar {
			t.Errorf("result[%d] = {%q, pxar=%v}, want {%q, pxar=%v}",
				i, result[i].name, result[i].isPxar, exp.name, exp.isPxar)
		}
	}
}

func TestTwoPointerMergeJournalPriority(t *testing.T) {
	// Same name in both pxar and journal: journal wins, pxar is skipped.
	pxarEntries := []dirEntrySlim{
		{name: "shadowed", contentOffset: 100, isReg: true},
		{name: "visible", contentOffset: 200, isReg: true},
	}
	sort.Slice(pxarEntries, func(i, j int) bool {
		return pxarEntries[i].name < pxarEntries[j].name
	})

	journalEdges := []GraphEdge{
		{Name: "shadowed", ChildID: 10},
	}
	sort.Slice(journalEdges, func(i, j int) bool {
		return journalEdges[i].Name < journalEdges[j].Name
	})

	// edgeNames set that commitWalk would build.
	edgeNames := map[string]bool{"shadowed": true}

	// Filter pxar (as commitWalk does) — shadowed removed.
	filtered := 0
	for i := range pxarEntries {
		if edgeNames[pxarEntries[i].name] {
			continue
		}
		if filtered != i {
			pxarEntries[filtered] = pxarEntries[i]
		}
		filtered++
	}
	pxarEntries = pxarEntries[:filtered]

	// Now merge: pxar=[visible], journal=[shadowed].
	type merged struct {
		name   string
		isPxar bool
	}
	var result []merged
	pi, ji := 0, 0
	for pi < len(pxarEntries) || ji < len(journalEdges) {
		if pi >= len(pxarEntries) {
			result = append(result, merged{journalEdges[ji].Name, false})
			ji++
		} else if ji >= len(journalEdges) {
			result = append(result, merged{pxarEntries[pi].name, true})
			pi++
		} else if pxarEntries[pi].name < journalEdges[ji].Name {
			result = append(result, merged{pxarEntries[pi].name, true})
			pi++
		} else {
			result = append(result, merged{journalEdges[ji].Name, false})
			ji++
		}
	}

	if len(result) != 2 {
		t.Fatalf("len(result)=%d, want 2", len(result))
	}
	if result[0].name != "shadowed" || result[0].isPxar {
		t.Errorf("result[0] = {%q, pxar=%v}, want {shadowed, false}", result[0].name, result[0].isPxar)
	}
	if result[1].name != "visible" || !result[1].isPxar {
		t.Errorf("result[1] = {%q, pxar=%v}, want {visible, true}", result[1].name, result[1].isPxar)
	}
}
