package pxarmount

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func TestJournal_UpsertGet(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path:    "/test/file.txt",
		State:   StateNew,
		Mode:    uint32(syscall.S_IFREG | 0o644),
		UID:     1000,
		GID:     1000,
		Size:    42,
		MtimeNs: time.Now().UnixNano(),
		CtimeNs: time.Now().UnixNano(),
		HasData: true,
	}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}

	got, err := mfs.journal.GetEntry("/test/file.txt")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if got == nil {
		t.Fatal("GetEntry returned nil")
	}
	if got.State != StateNew {
		t.Errorf("state = %q, want %q", got.State, StateNew)
	}
	if got.Mode != je.Mode {
		t.Errorf("mode = %o, want %o", got.Mode, je.Mode)
	}
	if got.UID != 1000 {
		t.Errorf("uid = %d, want %d", got.UID, 1000)
	}
	if !got.HasData {
		t.Error("has_data should be true")
	}
}

func TestJournal_Whiteout(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	if err := mfs.journal.MarkWhiteout("/deleted.txt"); err != nil {
		t.Fatalf("MarkWhiteout: %v", err)
	}

	je, err := mfs.journal.GetEntry("/deleted.txt")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if je == nil {
		t.Fatal("GetEntry returned nil for whiteout")
	}
	if je.State != StateWhiteout {
		t.Errorf("state = %q, want %q", je.State, StateWhiteout)
	}
}

func TestJournal_XAttrs(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	// Create an entry first.
	je := &JournalEntry{
		Path:  "/file.txt",
		State: StateNew,
		Mode:  uint32(syscall.S_IFREG | 0o644),
	}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}

	// Set xattrs.
	if err := mfs.journal.SetXAttr("/file.txt", "user.test", []byte("hello")); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}
	if err := mfs.journal.SetXAttr("/file.txt", "user.foo", []byte("bar")); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}

	// Get xattr.
	val, err := mfs.journal.GetXAttr("/file.txt", "user.test")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if string(val) != "hello" {
		t.Errorf("value = %q, want %q", val, "hello")
	}

	// List xattrs.
	names, err := mfs.journal.ListXAttrs("/file.txt")
	if err != nil {
		t.Fatalf("ListXAttrs: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("len(names) = %d, want 2", len(names))
	}

	// Remove xattr.
	if err := mfs.journal.RemoveXAttr("/file.txt", "user.test"); err != nil {
		t.Fatalf("RemoveXAttr: %v", err)
	}
	val, _ = mfs.journal.GetXAttr("/file.txt", "user.test")
	if val != nil {
		t.Error("xattr should be nil after removal")
	}
}

func TestJournal_ListDir(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	for _, p := range []string{"/dir/a.txt", "/dir/b.txt", "/other/c.txt"} {
		je := &JournalEntry{
			Path:  p,
			State: StateNew,
			Mode:  uint32(syscall.S_IFREG | 0o644),
		}
		if err := mfs.journal.MarkNew(je); err != nil {
			t.Fatalf("MarkNew %q: %v", p, err)
		}
	}

	children, err := mfs.journal.ListDir("/dir")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(children) != 2 {
		t.Errorf("len(children) = %d, want 2", len(children))
	}

	_, err = mfs.journal.ListDir("/empty")
	if err != nil {
		t.Fatalf("ListDir on empty: %v", err)
	}
	// Listing an empty dir should return zero results, not error.
}

func TestJournal_Rename(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path:    "/old.txt",
		State:   StateNew,
		Mode:    uint32(syscall.S_IFREG | 0o644),
		HasData: true,
	}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}

	if err := mfs.journal.Rename("/old.txt", "/new.txt", false); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	old, _ := mfs.journal.GetEntry("/old.txt")
	if old != nil {
		t.Error("old path should be removed")
	}

	got, err := mfs.journal.GetEntry("/new.txt")
	if err != nil {
		t.Fatalf("GetEntry new: %v", err)
	}
	if got == nil {
		t.Fatal("new path should exist")
	}
	if !got.HasData {
		t.Error("has_data should be preserved after rename")
	}
}

func TestJournal_Clear(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{Path: "/a.txt", State: StateNew}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}
	if err := mfs.journal.SetXAttr("/a.txt", "user.x", []byte("v")); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}

	if err := mfs.journal.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	entries, err := mfs.journal.AllEntries()
	if err != nil {
		t.Fatalf("AllEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("entries after clear = %d, want 0", len(entries))
	}

	xattrs, err := mfs.journal.AllXAttrs()
	if err != nil {
		t.Fatalf("AllXAttrs: %v", err)
	}
	if len(xattrs) != 0 {
		t.Errorf("xattrs after clear = %d, want 0", len(xattrs))
	}

	// Should be able to write again after clear.
	if err := mfs.journal.MarkNew(&JournalEntry{Path: "/after.txt", State: StateNew}); err != nil {
		t.Fatalf("MarkNew after clear: %v", err)
	}
}

func TestJournal_AppendReopen(t *testing.T) {
	dir := t.TempDir()
	journalDir := dir + "/" + JournalDir

	j1, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatalf("OpenJournal 1: %v", err)
	}
	if err := j1.MarkNew(&JournalEntry{Path: "/first.txt", State: StateNew}); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}
	j1.Close()

	j2, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatalf("OpenJournal 2: %v", err)
	}
	defer j2.Close()

	if err := j2.MarkNew(&JournalEntry{Path: "/second.txt", State: StateNew}); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}

	entries, err := j2.AllEntries()
	if err != nil {
		t.Fatalf("AllEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("entries after reopen = %d, want 2", len(entries))
	}

	// Check both entries are there.
	paths := make(map[string]bool)
	for _, e := range entries {
		paths[e.Path] = true
	}
	if !paths["/first.txt"] || !paths["/second.txt"] {
		t.Error("both entries should survive reopen")
	}
}

func TestJournal_ModifiedState(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path:    "/mod.txt",
		State:   StateModified,
		Mode:    uint32(syscall.S_IFREG | 0o755),
		MtimeNs: time.Now().UnixNano(),
	}
	if err := mfs.journal.MarkModified(je); err != nil {
		t.Fatalf("MarkModified: %v", err)
	}

	got, err := mfs.journal.GetEntry("/mod.txt")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if got.State != StateModified {
		t.Errorf("state = %q, want %q", got.State, StateModified)
	}
}

func TestJournal_RemoveEntry(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	if err := mfs.journal.MarkNew(&JournalEntry{Path: "/rm.txt", State: StateNew}); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}
	if err := mfs.journal.RemoveEntry("/rm.txt"); err != nil {
		t.Fatalf("RemoveEntry: %v", err)
	}

	got, _ := mfs.journal.GetEntry("/rm.txt")
	if got != nil {
		t.Error("entry should be nil after removal")
	}
}

func TestJournal_Opaque(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path:  "/opaque_dir",
		State: StateOpaque,
		Mode:  uint32(syscall.S_IFDIR | 0o755),
	}
	if err := mfs.journal.MarkOpaque("/opaque_dir", je); err != nil {
		t.Fatalf("MarkOpaque: %v", err)
	}

	got, err := mfs.journal.GetEntry("/opaque_dir")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if got.State != StateOpaque {
		t.Errorf("state = %q, want %q", got.State, StateOpaque)
	}

	isOpaque, err := mfs.journal.IsOpaque("/opaque_dir")
	if err != nil {
		t.Fatalf("IsOpaque: %v", err)
	}
	if !isOpaque {
		t.Error("IsOpaque should return true")
	}

	isOpaque, _ = mfs.journal.IsOpaque("/nonexistent")
	if isOpaque {
		t.Error("IsOpaque should return false for non-existent path")
	}
}

func TestJournal_AllEntries(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	paths := []string{"/a.txt", "/b/c.txt", "/z.txt"}
	for _, p := range paths {
		if err := mfs.journal.MarkNew(&JournalEntry{Path: p, State: StateNew}); err != nil {
			t.Fatalf("MarkNew %q: %v", p, err)
		}
	}

	entries, err := mfs.journal.AllEntries()
	if err != nil {
		t.Fatalf("AllEntries: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("len(entries) = %d, want 3", len(entries))
	}

	// Verify ordering: entries should be ordered by path.
	for i := 1; i < len(entries); i++ {
		if entries[i].Path < entries[i-1].Path {
			t.Errorf("entries not ordered: %q before %q", entries[i-1].Path, entries[i].Path)
		}
	}
}

func TestJournal_SetHasData(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{Path: "/copied.txt", State: StateModified, Mode: uint32(syscall.S_IFREG | 0o644)}
	if err := mfs.journal.MarkModified(je); err != nil {
		t.Fatalf("MarkModified: %v", err)
	}

	if err := mfs.journal.SetHasData("/copied.txt"); err != nil {
		t.Fatalf("SetHasData: %v", err)
	}

	got, _ := mfs.journal.GetEntry("/copied.txt")
	if !got.HasData {
		t.Error("has_data should be true after SetHasData")
	}
}

func TestJournal_NonexistentReturnsNil(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je, err := mfs.journal.GetEntry("/nonexistent")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if je != nil {
		t.Error("GetEntry should return nil for non-existent path")
	}
}

func TestJournal_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	journalDir := dir + "/" + JournalDir

	j, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	defer j.Close()

	const goroutines = 10
	const perGoroutine = 20
	errs := make(chan error, goroutines)

	for i := range goroutines {
		go func(id int) {
			for k := range perGoroutine {
				path := "/concurrent/file_" + string(rune('a'+id)) + "_" + string(rune(k))
				je := &JournalEntry{
					Path:  path,
					State: StateNew,
					Mode:  uint32(syscall.S_IFREG | 0o644),
				}
				if err := j.MarkNew(je); err != nil {
					errs <- err
					return
				}
			}
			errs <- nil
		}(i)
	}

	for range goroutines {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}

	entries, err := j.AllEntries()
	if err != nil {
		t.Fatalf("AllEntries: %v", err)
	}
	if len(entries) != goroutines*perGoroutine {
		t.Errorf("entries = %d, want %d", len(entries), goroutines*perGoroutine)
	}
}

func TestJournal_SymlinkTarget(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path:   "/mylink",
		State:  StateNew,
		Mode:   uint32(syscall.S_IFLNK | 0o777),
		Target: "/some/target/path",
	}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}

	got, _ := mfs.journal.GetEntry("/mylink")
	if got.Target != "/some/target/path" {
		t.Errorf("target = %q, want %q", got.Target, "/some/target/path")
	}
}

func TestJournal_RenameWithXAttrs(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{Path: "/old.txt", State: StateNew}
	if err := mfs.journal.MarkNew(je); err != nil {
		t.Fatalf("MarkNew: %v", err)
	}
	if err := mfs.journal.SetXAttr("/old.txt", "user.key", []byte("val")); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}

	if err := mfs.journal.Rename("/old.txt", "/new.txt", false); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Xattr should be moved too.
	val, err := mfs.journal.GetXAttr("/new.txt", "user.key")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if string(val) != "val" {
		t.Errorf("xattr value = %q, want %q", val, "val")
	}

	// Old xattr should be gone.
	val, _ = mfs.journal.GetXAttr("/old.txt", "user.key")
	if val != nil {
		t.Error("old xattr should be removed")
	}
}

func TestJournal_RenameImmutable(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	je := &JournalEntry{
		Path: "/renamed.txt",
		Mode: uint32(syscall.S_IFREG | 0o644),
		UID:  1000,
		GID:  1000,
		Size: 1024,
	}
	if err := mfs.journal.RenameImmutable("/src.txt", "/renamed.txt", je); err != nil {
		t.Fatalf("RenameImmutable: %v", err)
	}

	// Old path should be whiteout.
	src, _ := mfs.journal.GetEntry("/src.txt")
	if src == nil || src.State != StateWhiteout {
		t.Errorf("source should be whiteout, got %v", src)
	}

	// New path should have the entry.
	dst, _ := mfs.journal.GetEntry("/renamed.txt")
	if dst == nil {
		t.Fatal("destination should exist")
	}
	if dst.Mode != 0o644 {
		t.Errorf("mode = %o, want 0o644", dst.Mode)
	}
	if dst.State != StateModified {
		t.Errorf("state = %q, want %q", dst.State, StateModified)
	}
}

func TestJournal_NonExistentXAttr(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	// Non-existent entry: xattr should not error.
	err := mfs.journal.SetXAttr("/nonexistent", "user.x", []byte("v"))
	if err != nil {
		t.Fatalf("SetXAttr should succeed regardless of entry: %v", err)
	}

	// Just the row exists in xattrs table.
	val, _ := mfs.journal.GetXAttr("/nonexistent", "user.x")
	if string(val) != "v" {
		t.Error("xattr should be stored independently")
	}
}

func TestJournal_ReadDirSorted(t *testing.T) {
	mfs, _, cleanup := newTestMutableFS(t)
	defer cleanup()

	children := []string{"/dir/z.txt", "/dir/a.txt", "/dir/m.txt"}
	for _, p := range children {
		if err := mfs.journal.MarkNew(&JournalEntry{Path: p, State: StateNew}); err != nil {
			t.Fatalf("MarkNew %q: %v", p, err)
		}
	}

	entries, err := mfs.journal.ListDir("/dir")
	if err != nil {
		t.Fatalf("ListDir: %v", err)
	}
	if len(entries) != 3 {
		t.Fatal("expected 3 children")
	}

	// Should be sorted by name.
	prev := ""
	for _, e := range entries {
		name := e.Path[len("/dir/"):]
		if name < prev {
			t.Errorf("not sorted: %q before %q", prev, name)
		}
		prev = name
	}
}

func TestJournal_CorruptDBRecovery(t *testing.T) {
	dir := t.TempDir()
	journalDir := dir + "/" + JournalDir

	// Corrupt the database file before opening.
	dbPath := journalDir + "/journal.db"
	if err := os.MkdirAll(journalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dbPath, []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := OpenJournal(journalDir)
	if err == nil {
		t.Error("should fail when DB file is corrupted")
	}
}

func BenchmarkJournal_Upsert(b *testing.B) {
	dir := b.TempDir()
	journalDir := dir + "/" + JournalDir
	j, err := OpenJournal(journalDir)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()

	b.ResetTimer()
	for i := range b.N {
		path := "/bench/file_" + string(rune(i%26+'a')) + ".txt"
		je := &JournalEntry{
			Path:  path,
			State: StateModified,
			Mode:  uint32(syscall.S_IFREG | 0o644),
		}
		_ = j.MarkModified(je)
	}
}

func BenchmarkJournal_GetEntry(b *testing.B) {
	dir := b.TempDir()
	journalDir := dir + "/" + JournalDir
	j, err := OpenJournal(journalDir)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()

	je := &JournalEntry{Path: "/bench/cached.txt", State: StateNew, Mode: uint32(syscall.S_IFREG | 0o644)}
	_ = j.MarkNew(je)

	b.ResetTimer()
	for range b.N {
		_, _ = j.GetEntry("/bench/cached.txt")
	}
}

func BenchmarkJournal_ListDir(b *testing.B) {
	dir := b.TempDir()
	journalDir := dir + "/" + JournalDir
	j, err := OpenJournal(journalDir)
	if err != nil {
		b.Fatal(err)
	}
	defer j.Close()

	// Create 100 children in /bench.
	for i := range 100 {
		path := "/bench/file_" + string(rune(i%26+'a')) + "_" + string(nameForBench(i))
		_ = j.MarkNew(&JournalEntry{Path: path, State: StateNew})
	}

	b.ResetTimer()
	for range b.N {
		_, _ = j.ListDir("/bench")
	}
}

func nameForBench(i int) string {
	const base = 'a'
	buf := make([]byte, 4)
	for k := range 4 {
		buf[3-k] = byte(base + (i % 26))
		i /= 26
	}
	return string(buf)
}
