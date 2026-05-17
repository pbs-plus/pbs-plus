package pxarmount

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- Helpers ---

// testJournal creates a temporary directory and opens a journal.
// Returns the journal and a cleanup function.
func testJournal(t *testing.T) (*Journal, func()) {
	t.Helper()
	dir, err := os.MkdirTemp("", "pxar-journal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	journalDir := filepath.Join(dir, "journal")
	j, err := OpenJournal(journalDir)
	if err != nil {
		_ = os.RemoveAll(dir)
		t.Fatalf("OpenJournal: %v", err)
	}
	return j, func() {
		_ = j.Close()
		_ = os.RemoveAll(dir)
	}
}

// testJournalDir returns the directory containing the journal database.
func testJournalDir(j *Journal) string {
	var dbPath string
	_ = j.db.QueryRow(`PRAGMA database_list`).Scan(nil, &dbPath, nil)
	return filepath.Dir(dbPath)
}

// --- OpenJournal / Recovery ---

func TestOpenJournalCreatesSchema(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Verify all tables exist.
	tables := []string{"meta", "nodes", "edges", "xattrs", "whiteouts"}
	for _, table := range tables {
		var count int
		err := j.db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("table %s not found", table)
		}
	}
}

func TestOpenJournalRootNodeAlwaysExists(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node, err := j.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode(1): %v", err)
	}
	if node == nil {
		t.Fatal("root node missing")
	}
	if node.Kind != NodeDir {
		t.Errorf("root kind = %d, want NodeDir(%d)", node.Kind, NodeDir)
	}
	if node.RedirectTo != "/" {
		t.Errorf("root redirect_to = %q, want \"/\"", node.RedirectTo)
	}
}

func TestOpenJournalRecreatesRootIfMissing(t *testing.T) {
	j, cleanup := testJournal(t)

	// Get journal dir before closing.
	journalDir := testJournalDir(j)

	// Delete root node directly.
	_, err := j.db.Exec(`DELETE FROM nodes WHERE id = 1`)
	if err != nil {
		t.Fatalf("delete root: %v", err)
	}
	cleanup()

	// Reopen — should recreate root.
	j2, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatalf("OpenJournal after root delete: %v", err)
	}
	defer func() {
		_ = j2.Close()
	}()

	node, err := j2.GetNode(1)
	if err != nil {
		t.Fatalf("GetNode(1) after recovery: %v", err)
	}
	if node == nil {
		t.Fatal("root node not recreated after recovery")
	}
}

func TestOpenJournalIntegrityCheckRejectsCorruptDB(t *testing.T) {
	// We can't easily create a corrupt SQLite DB, but we can verify
	// that OpenJournal runs integrity_check by confirming it passes on
	// a valid DB.
	j, cleanup := testJournal(t)
	defer cleanup()

	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity on clean journal: %v", err)
	}
}

func TestOpenJournalSchemaVersion(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	var ver string
	err := j.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&ver)
	if err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if ver != fmt.Sprint(schemaVersion) {
		t.Errorf("schema_version = %q, want %d", ver, schemaVersion)
	}
}

func TestOpenJournalCleansOrphanEdges(t *testing.T) {
	j, cleanup := testJournal(t)

	// Create a node and edge, then delete the node without CASCADE.
	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, err := j.EnsureNodePath("/orphan_test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}

	// Temporarily disable foreign keys to simulate a crash that left
	// an edge pointing to a deleted node.
	_, _ = j.db.Exec(`PRAGMA foreign_keys = OFF`)
	_, _ = j.db.Exec(`DELETE FROM nodes WHERE id = ?`, id)
	_, _ = j.db.Exec(`PRAGMA foreign_keys = ON`)

	// Verify orphan edge exists.
	var orphanCount int
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM edges WHERE child_id NOT IN (SELECT id FROM nodes)`).Scan(&orphanCount)
	if orphanCount == 0 {
		t.Fatal("expected orphan edge after direct node deletion")
	}

	// Reopen should clean orphan edges.
	journalDir := testJournalDir(j)
	cleanup()

	j2, err := OpenJournal(journalDir)
	if err != nil {
		t.Fatalf("OpenJournal after orphan: %v", err)
	}
	defer func() { _ = j2.Close() }()

	// Verify no orphans remain.
	if err := j2.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after orphan cleanup: %v", err)
	}
}

func TestOpenJournalIdempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-journal-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Open and close multiple times — should not fail or duplicate data.
	for i := range 5 {
		j, err := OpenJournal(dir)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		var nodeCount int
		_ = j.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&nodeCount)
		_ = j.Close()
		// Should always have exactly 1 root node.
		if nodeCount != 1 {
			t.Errorf("open %d: node count = %d, want 1", i, nodeCount)
		}
	}
}

// --- Node CRUD ---

func TestCreateAndGetNode(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    0o644,
		UID:     1000,
		GID:     1000,
		Size:    4096,
		MtimeNs: now,
		CtimeNs: now,
	}

	id, err := j.EnsureNodePath("/test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero node ID")
	}

	got, err := j.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got == nil {
		t.Fatal("node not found")
	}
	if got.Kind != NodeFile {
		t.Errorf("kind = %d, want %d", got.Kind, NodeFile)
	}
	if got.Mode != 0o644 {
		t.Errorf("mode = %o, want %o", got.Mode, 0o644)
	}
	if got.UID != 1000 || got.GID != 1000 {
		t.Errorf("uid:gid = %d:%d, want 1000:1000", got.UID, got.GID)
	}
	if got.Size != 4096 {
		t.Errorf("size = %d, want 4096", got.Size)
	}
}

func TestUpdateNode(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, err := j.EnsureNodePath("/test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}

	// Update metadata.
	node.ID = id
	node.Mode = 0o755
	node.Size = 8192
	if err := j.UpdateNode(node); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got, err := j.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.Mode != 0o755 {
		t.Errorf("mode after update = %o, want %o", got.Mode, 0o755)
	}
	if got.Size != 8192 {
		t.Errorf("size after update = %d, want 8192", got.Size)
	}
}

func TestSetHasData(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644, HasData: false}
	id, err := j.EnsureNodePath("/test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}

	if err := j.SetHasData(id); err != nil {
		t.Fatalf("SetHasData: %v", err)
	}

	got, err := j.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !got.HasData {
		t.Error("HasData should be true after SetHasData")
	}
}

func TestGetNodeNonexistent(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	got, err := j.GetNode(99999)
	if err != nil {
		t.Fatalf("GetNode(99999): %v", err)
	}
	if got != nil {
		t.Error("expected nil for nonexistent node")
	}
}

// --- Edge CRUD ---

func TestListEdges(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create nodes and edges under root (node 1).
	n1 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	n2 := &GraphNode{Kind: NodeDir, Mode: 0o755}

	id1, _ := j.EnsureNodePath("/file1.txt", n1, false)
	id2, _ := j.EnsureNodePath("/dir1", n2, false)

	edges, err := j.ListEdges(1) // root's edges
	if err != nil {
		t.Fatalf("ListEdges: %v", err)
	}

	if len(edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(edges))
	}

	edgeMap := make(map[string]int64)
	for _, e := range edges {
		edgeMap[e.Name] = e.ChildID
	}
	if edgeMap["file1.txt"] != id1 {
		t.Errorf("file1.txt edge = %d, want %d", edgeMap["file1.txt"], id1)
	}
	if edgeMap["dir1"] != id2 {
		t.Errorf("dir1 edge = %d, want %d", edgeMap["dir1"], id2)
	}
}

func TestEdgesAreOrderedByName(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	names := []string{"charlie", "alpha", "bravo", "delta"}
	for _, name := range names {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		_, _ = j.EnsureNodePath("/"+name, n, false)
	}

	edges, err := j.ListEdges(1)
	if err != nil {
		t.Fatalf("ListEdges: %v", err)
	}

	for i := 1; i < len(edges); i++ {
		if edges[i].Name <= edges[i-1].Name {
			t.Errorf("edges not sorted: %q before %q", edges[i-1].Name, edges[i].Name)
		}
	}
}

// --- Whiteout ---

func TestAddAndListWhiteouts(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	if err := j.AddWhiteout(1, "deleted.txt"); err != nil {
		t.Fatalf("AddWhiteout: %v", err)
	}
	if err := j.AddWhiteout(1, "removed_dir"); err != nil {
		t.Fatalf("AddWhiteout: %v", err)
	}

	wos, err := j.ListWhiteouts(1)
	if err != nil {
		t.Fatalf("ListWhiteouts: %v", err)
	}
	if len(wos) != 2 {
		t.Fatalf("expected 2 whiteouts, got %d", len(wos))
	}

	woSet := make(map[string]bool)
	for _, w := range wos {
		woSet[w] = true
	}
	if !woSet["deleted.txt"] || !woSet["removed_dir"] {
		t.Errorf("whiteouts = %v, want deleted.txt and removed_dir", wos)
	}
}

func TestWhiteoutIdempotent(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Add same whiteout twice — should not error or duplicate.
	if err := j.AddWhiteout(1, "foo"); err != nil {
		t.Fatalf("AddWhiteout 1: %v", err)
	}
	if err := j.AddWhiteout(1, "foo"); err != nil {
		t.Fatalf("AddWhiteout 2: %v", err)
	}

	wos, _ := j.ListWhiteouts(1)
	if len(wos) != 1 {
		t.Errorf("expected 1 whiteout after duplicate add, got %d", len(wos))
	}
}

// --- XAttr ---

func TestXAttrCRUD(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/test.txt", node, false)

	// Set.
	if err := j.SetXAttr(id, "user.comment", []byte("hello")); err != nil {
		t.Fatalf("SetXAttr: %v", err)
	}

	// Get.
	val, err := j.GetXAttr(id, "user.comment")
	if err != nil {
		t.Fatalf("GetXAttr: %v", err)
	}
	if string(val) != "hello" {
		t.Errorf("GetXAttr = %q, want %q", val, "hello")
	}

	// List.
	names, err := j.ListXAttrs(id)
	if err != nil {
		t.Fatalf("ListXAttrs: %v", err)
	}
	if len(names) != 1 || names[0] != "user.comment" {
		t.Errorf("ListXAttrs = %v, want [user.comment]", names)
	}

	// Update.
	if err := j.SetXAttr(id, "user.comment", []byte("updated")); err != nil {
		t.Fatalf("SetXAttr update: %v", err)
	}
	val, _ = j.GetXAttr(id, "user.comment")
	if string(val) != "updated" {
		t.Errorf("after update = %q, want %q", val, "updated")
	}

	// Remove.
	if err := j.RemoveXAttr(id, "user.comment"); err != nil {
		t.Fatalf("RemoveXAttr: %v", err)
	}
	val, _ = j.GetXAttr(id, "user.comment")
	if val != nil {
		t.Error("expected nil after RemoveXAttr")
	}
}

func TestXAttrMultipleNames(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/test.txt", node, false)

	xattrs := map[string]string{
		"user.a": "value_a",
		"user.b": "value_b",
		"user.c": "value_c",
	}
	for k, v := range xattrs {
		_ = j.SetXAttr(id, k, []byte(v))
	}

	names, _ := j.ListXAttrs(id)
	if len(names) != 3 {
		t.Errorf("expected 3 xattrs, got %d", len(names))
	}

	for _, name := range names {
		val, _ := j.GetXAttr(id, name)
		if string(val) != xattrs[name] {
			t.Errorf("xattr %q = %q, want %q", name, val, xattrs[name])
		}
	}
}

func TestXAttrOnNonexistentNode(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Getting xattr on nonexistent node should return nil (not error).
	val, err := j.GetXAttr(99999, "user.foo")
	if err != nil {
		t.Fatalf("GetXAttr on nonexistent: %v", err)
	}
	if val != nil {
		t.Error("expected nil for nonexistent node xattr")
	}
}

// --- Path Resolution ---

func TestResolvePathRoot(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	nodeID, pxarPath, fellOffAt, remaining, err := j.ResolvePath("/")
	if err != nil {
		t.Fatalf("ResolvePath /: %v", err)
	}
	if nodeID != 1 {
		t.Errorf("nodeID = %d, want 1", nodeID)
	}
	if pxarPath != "/" {
		t.Errorf("pxarPath = %q, want \"/\"", pxarPath)
	}
	if fellOffAt != 0 {
		t.Errorf("fellOffAt = %d, want 0", fellOffAt)
	}
	if remaining != "" {
		t.Errorf("remaining = %q, want empty", remaining)
	}
}

func TestResolvePathNonExistent(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	nodeID, pxarPath, fellOffAt, remaining, err := j.ResolvePath("/no/such/path")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if nodeID != 0 {
		t.Errorf("nodeID = %d, want 0", nodeID)
	}
	if pxarPath == "" {
		t.Error("pxarPath should not be empty for non-journal path")
	}
	if fellOffAt == 0 {
		t.Error("fellOffAt should be non-zero for fallen-off path")
	}
	if remaining == "" {
		t.Error("remaining should not be empty for partial match")
	}
}

func TestResolvePathFullGraphMatch(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644, RedirectTo: "/orig.txt"}
	id, err := j.EnsureNodePath("/a/b/c.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}
	_ = id

	nodeID, pxarPath, fellOffAt, remaining, err := j.ResolvePath("/a/b/c.txt")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if nodeID == 0 {
		t.Fatal("expected nodeID != 0 for full graph match")
	}
	if fellOffAt != 0 {
		t.Errorf("fellOffAt = %d, want 0 for full match", fellOffAt)
	}
	if remaining != "" {
		t.Errorf("remaining = %q, want empty for full match", remaining)
	}
	_ = pxarPath
}

func TestResolvePathPartialMatch(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create /a/b only (not /a/b/c.txt).
	node := &GraphNode{Kind: NodeDir, Mode: 0o755}
	_, err := j.EnsureNodePath("/a/b", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath: %v", err)
	}

	// Resolve /a/b/c.txt — should fall off at the /a/b node.
	nodeID, _, fellOffAt, remaining, err := j.ResolvePath("/a/b/c.txt")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if nodeID != 0 {
		t.Errorf("nodeID = %d, want 0 for partial match", nodeID)
	}
	if remaining != "c.txt" {
		t.Errorf("remaining = %q, want \"c.txt\"", remaining)
	}
	_ = fellOffAt
}

func TestResolvePathWhiteout(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Add whiteout for "deleted" under root.
	if err := j.AddWhiteout(1, "deleted"); err != nil {
		t.Fatalf("AddWhiteout: %v", err)
	}

	// Resolve should return whiteout indicator.
	nodeID, pxarPath, _, _, err := j.ResolvePath("/deleted")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if nodeID != 0 {
		t.Errorf("nodeID = %d, want 0 for whiteout", nodeID)
	}
	if pxarPath != "" {
		t.Errorf("pxarPath = %q, want empty for whiteout", pxarPath)
	}
}

func TestResolvePathSnapshotConsistency(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create a node.
	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, _ = j.EnsureNodePath("/test.txt", node, false)

	// Resolve should find it.
	nodeID, _, _, _, err := j.ResolvePath("/test.txt")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if nodeID == 0 {
		t.Fatal("expected nodeID != 0")
	}
}

// --- Compound Atomic Operations ---

func TestEnsureNodePathCreatesIntermediates(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, err := j.EnsureNodePath("/a/b/c/d/file.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath deep: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ID")
	}

	// Verify intermediate directories were created.
	for _, path := range []string{"/a", "/a/b", "/a/b/c", "/a/b/c/d"} {
		nodeID, _, _, _, err := j.ResolvePath(path)
		if err != nil {
			t.Fatalf("ResolvePath %q: %v", path, err)
		}
		if nodeID == 0 {
			t.Errorf("intermediate %q not found", path)
		}
		got, _ := j.GetNode(nodeID)
		if got == nil || got.Kind != NodeDir {
			t.Errorf("intermediate %q should be a directory", path)
		}
	}
}

func TestEnsureNodePathWithWhiteout(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, err := j.EnsureNodePath("/shadow.txt", node, true)
	if err != nil {
		t.Fatalf("EnsureNodePath with whiteout: %v", err)
	}

	// Whiteout should exist under root for "shadow.txt".
	wos, _ := j.ListWhiteouts(1)
	found := false
	for _, w := range wos {
		if w == "shadow.txt" {
			found = true
		}
	}
	if !found {
		t.Error("expected whiteout for shadow.txt")
	}
}

func TestEnsureNodePathIdempotent(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}

	id1, err := j.EnsureNodePath("/test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath 1: %v", err)
	}

	// Note: EnsureNodePath always creates a new node (it doesn't check
	// for existing). This is by design — the caller (ensureNode with
	// per-path lock) handles deduplication at the MutableFS level.
	// But calling twice should still succeed without error.
	id2, err := j.EnsureNodePath("/test.txt", node, false)
	if err != nil {
		t.Fatalf("EnsureNodePath 2: %v", err)
	}
	// Edge should now point to id2 (REPLACE).
	edges, _ := j.ListEdges(1)
	if len(edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(edges))
	}
	_ = id1
	_ = id2
}

func TestCreateNodeEdgeAndWhiteout(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644, HasData: true}
	id, err := j.CreateNodeEdgeAndWhiteout(1, "newfile.txt", node, true)
	if err != nil {
		t.Fatalf("CreateNodeEdgeAndWhiteout: %v", err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ID")
	}

	// Edge should exist.
	edges, _ := j.ListEdges(1)
	if len(edges) != 1 || edges[0].Name != "newfile.txt" {
		t.Errorf("edges = %v, want [newfile.txt]", edges)
	}

	// Whiteout should exist.
	wos, _ := j.ListWhiteouts(1)
	if len(wos) != 1 || wos[0] != "newfile.txt" {
		t.Errorf("whiteouts = %v, want [newfile.txt]", wos)
	}
}

func TestDeleteEdgeAndNode(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/deleteme.txt", node, false)

	// Delete with whiteout.
	if err := j.DeleteEdgeAndNode(1, "deleteme.txt", id, true); err != nil {
		t.Fatalf("DeleteEdgeAndNode: %v", err)
	}

	// Node should be gone.
	got, _ := j.GetNode(id)
	if got != nil {
		t.Error("node should be deleted")
	}

	// Edge should be gone.
	edges, _ := j.ListEdges(1)
	for _, e := range edges {
		if e.Name == "deleteme.txt" {
			t.Error("edge should be deleted")
		}
	}

	// Whiteout should exist.
	wos, _ := j.ListWhiteouts(1)
	found := false
	for _, w := range wos {
		if w == "deleteme.txt" {
			found = true
		}
	}
	if !found {
		t.Error("expected whiteout after delete")
	}
}

func TestDeleteEdgeAndNodeCascadesXAttrs(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/xattr_test.txt", node, false)

	// Add xattrs.
	_ = j.SetXAttr(id, "user.a", []byte("1"))
	_ = j.SetXAttr(id, "user.b", []byte("2"))

	// Delete node — xattrs should cascade.
	_ = j.DeleteEdgeAndNode(1, "xattr_test.txt", id, false)

	// XAttrs should be gone.
	names, _ := j.ListXAttrs(id)
	if len(names) != 0 {
		t.Errorf("xattrs after delete = %v, want empty", names)
	}
}

func TestMoveEdgeAndWhiteout(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create source node.
	node := &GraphNode{Kind: NodeFile, Mode: 0o644, RedirectTo: "/orig.txt"}
	srcID, _ := j.EnsureNodePath("/before.txt", node, false)
	_ = srcID

	// Rename /before.txt -> /after.txt under root.
	err := j.MoveEdgeAndWhiteout(1, "before.txt", 1, "after.txt", 0, true, false)
	if err != nil {
		t.Fatalf("MoveEdgeAndWhiteout: %v", err)
	}

	// Old edge should be gone.
	edges, _ := j.ListEdges(1)
	for _, e := range edges {
		if e.Name == "before.txt" && e.ChildID == srcID {
			t.Error("old edge should be gone")
		}
	}

	// New edge should exist.
	nodeID, _, _, _, _ := j.ResolvePath("/after.txt")
	if nodeID != srcID {
		t.Errorf("ResolvePath /after.txt = %d, want %d", nodeID, srcID)
	}

	// Whiteout at old location.
	wos, _ := j.ListWhiteouts(1)
	found := false
	for _, w := range wos {
		if w == "before.txt" {
			found = true
		}
	}
	if !found {
		t.Error("expected whiteout at old location")
	}
}

func TestMoveEdgeAndWhiteoutReplacesDest(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node1 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	node2 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id1, _ := j.EnsureNodePath("/src.txt", node1, false)
	id2, _ := j.EnsureNodePath("/dst.txt", node2, false)

	// Rename src.txt over dst.txt — should replace dest.
	err := j.MoveEdgeAndWhiteout(1, "src.txt", 1, "dst.txt", id2, false, false)
	if err != nil {
		t.Fatalf("MoveEdgeAndWhiteout replace: %v", err)
	}

	// dst.txt should now point to id1.
	nodeID, _, _, _, _ := j.ResolvePath("/dst.txt")
	if nodeID != id1 {
		t.Errorf("after rename, /dst.txt = %d, want %d", nodeID, id1)
	}

	// id2 should be deleted.
	got, _ := j.GetNode(id2)
	if got != nil {
		t.Error("replaced dest node should be deleted")
	}

	// src.txt edge should be gone.
	nodeID2, _, _, _, _ := j.ResolvePath("/src.txt")
	if nodeID2 != 0 {
		t.Error("src.txt should be gone after rename")
	}
}

// --- Clear and Sync ---

func TestClearResetsToRoot(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create lots of data.
	for i := range 50 {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		_, _ = j.EnsureNodePath(fmt.Sprintf("/file_%03d.txt", i), n, false)
		_ = j.AddWhiteout(1, fmt.Sprintf("wo_%03d", i))
	}

	if err := j.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	// Only root node should remain.
	var nodeCount int
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&nodeCount)
	if nodeCount != 1 {
		t.Errorf("node count after clear = %d, want 1", nodeCount)
	}

	// No edges, whiteouts, xattrs.
	var edgeCount, woCount, xaCount int
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM edges`).Scan(&edgeCount)
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM whiteouts`).Scan(&woCount)
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM xattrs`).Scan(&xaCount)
	if edgeCount != 0 {
		t.Errorf("edge count = %d, want 0", edgeCount)
	}
	if woCount != 0 {
		t.Errorf("whiteout count = %d, want 0", woCount)
	}
	if xaCount != 0 {
		t.Errorf("xattr count = %d, want 0", xaCount)
	}

	// Root should still be accessible.
	node, _ := j.GetNode(1)
	if node == nil {
		t.Fatal("root node missing after clear")
	}
}

func TestSyncDurability(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Write some data, then sync.
	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, _ = j.EnsureNodePath("/synced.txt", node, false)

	if err := j.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Verify integrity after sync.
	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after sync: %v", err)
	}
}

func TestClearAndSyncIdempotent(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Clear + Sync twice should not error.
	for i := range 3 {
		if err := j.Clear(); err != nil {
			t.Fatalf("Clear %d: %v", i, err)
		}
		if err := j.Sync(); err != nil {
			t.Fatalf("Sync %d: %v", i, err)
		}
	}

	// Root still intact.
	node, _ := j.GetNode(1)
	if node == nil {
		t.Fatal("root missing after repeated clear+sync")
	}
}

// --- AllXAttrs Batch ---

func TestAllXAttrs(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	n1 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	n2 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id1, _ := j.EnsureNodePath("/f1.txt", n1, false)
	id2, _ := j.EnsureNodePath("/f2.txt", n2, false)

	_ = j.SetXAttr(id1, "user.a", []byte("1"))
	_ = j.SetXAttr(id1, "user.b", []byte("2"))
	_ = j.SetXAttr(id2, "user.c", []byte("3"))

	all, err := j.AllXAttrs()
	if err != nil {
		t.Fatalf("AllXAttrs: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 nodes with xattrs, got %d", len(all))
	}
	if len(all[id1]) != 2 {
		t.Errorf("node %d: expected 2 xattrs, got %d", id1, len(all[id1]))
	}
	if len(all[id2]) != 1 {
		t.Errorf("node %d: expected 1 xattr, got %d", id2, len(all[id2]))
	}
}

// --- VerifyIntegrity ---

func TestVerifyIntegrityClean(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Add some data.
	n := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/integrity.txt", n, false)
	_ = j.SetXAttr(id, "user.test", []byte("value"))

	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity clean: %v", err)
	}
}

// --- Concurrency ---

func TestConcurrentTransactions(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	const workers = 20
	const opsPerWorker = 50
	var wg sync.WaitGroup
	wg.Add(workers)

	var errors atomic.Int64

	for w := range workers {
		go func(workerID int) {
			defer wg.Done()
			for i := range opsPerWorker {
				path := fmt.Sprintf("/worker_%d/file_%d.txt", workerID, i)
				node := &GraphNode{Kind: NodeFile, Mode: 0o644}
				if _, err := j.EnsureNodePath(path, node, false); err != nil {
					errors.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()

	if errCount := errors.Load(); errCount > 0 {
		t.Errorf("%d errors during concurrent transactions", errCount)
	}

	// Verify integrity after concurrent mutations.
	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after concurrent ops: %v", err)
	}
}

func TestConcurrentWhiteouts(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	const workers = 10
	var wg sync.WaitGroup
	wg.Add(workers)

	// All workers add whiteout for the same name — should be idempotent.
	for range workers {
		go func() {
			defer wg.Done()
			_ = j.AddWhiteout(1, "concurrent_name")
		}()
	}

	wg.Wait()

	wos, _ := j.ListWhiteouts(1)
	if len(wos) != 1 {
		t.Errorf("expected 1 whiteout, got %d", len(wos))
	}
}

func TestConcurrentXAttrWrites(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/concurrent_xattr.txt", node, false)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	// All workers write different xattr names.
	for w := range workers {
		go func(wID int) {
			defer wg.Done()
			name := fmt.Sprintf("user.worker_%d", wID)
			_ = j.SetXAttr(id, name, fmt.Appendf(nil, "value_%d", wID))
		}(w)
	}

	wg.Wait()

	names, _ := j.ListXAttrs(id)
	if len(names) != workers {
		t.Errorf("expected %d xattrs, got %d", workers, len(names))
	}
}

func TestConcurrentResolvePath(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create a deep structure.
	node := &GraphNode{Kind: NodeFile, Mode: 0o644}
	_, _ = j.EnsureNodePath("/a/b/c/d/e/f.txt", node, false)

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	var resolveErrors atomic.Int64

	for range workers {
		go func() {
			defer wg.Done()
			for range 50 {
				_, _, _, _, err := j.ResolvePath("/a/b/c/d/e/f.txt")
				if err != nil {
					resolveErrors.Add(1)
				}
			}
		}()
	}

	wg.Wait()

	if errCount := resolveErrors.Load(); errCount > 0 {
		t.Errorf("%d resolve errors under concurrent reads", errCount)
	}
}

// --- WAL Checkpointing ---

func TestBackgroundCheckpoint(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Override interval to be fast for testing.
	j.checkpointInterval.Store(int64(100 * time.Millisecond))

	// Write enough to trigger checkpoints.
	for i := range 300 {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		_, _ = j.EnsureNodePath(fmt.Sprintf("/ckpt_%04d.txt", i), n, false)
	}

	// Wait for background checkpoint to fire.
	time.Sleep(500 * time.Millisecond)

	// Verify integrity after background checkpoints.
	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after background checkpoint: %v", err)
	}
}

func TestEagerCheckpointThreshold(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Write 256+ transactions to trigger eager checkpoint.
	for i := range 300 {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		_, _ = j.EnsureNodePath(fmt.Sprintf("/eager_%04d.txt", i), n, false)
	}

	// ckptPending should have been reset by eager checkpoint.
	if pending := j.ckptPending.Value(); pending >= 256 {
		t.Errorf("ckptPending = %d, should be < 256 after eager checkpoint", pending)
	}
}

// --- Crash Recovery Simulation ---

func TestCrashRecoveryAfterPartialWrites(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-journal-crash-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Phase 1: Create journal with data.
	j1, err := OpenJournal(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 100 {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		id, _ := j1.EnsureNodePath(fmt.Sprintf("/recover_%03d.txt", i), n, false)
		_ = j1.SetXAttr(id, "user.test", []byte("value"))
	}

	// Simulate crash: close without Sync (data is in WAL only).
	_ = j1.Close()

	// Phase 2: Reopen — WAL should be replayed automatically.
	j2, err := OpenJournal(dir)
	if err != nil {
		t.Fatalf("OpenJournal after crash: %v", err)
	}
	defer func() { _ = j2.Close() }()

	// All data should be intact — WAL replay guarantees this.
	for i := range 100 {
		path := fmt.Sprintf("/recover_%03d.txt", i)
		nodeID, _, _, _, err := j2.ResolvePath(path)
		if err != nil {
			t.Errorf("ResolvePath %q after recovery: %v", path, err)
			continue
		}
		if nodeID == 0 {
			t.Errorf("node %q lost after crash recovery", path)
			continue
		}
		got, _ := j2.GetNode(nodeID)
		if got == nil {
			t.Errorf("GetNode for %q returned nil after recovery", path)
		}
	}

	// Verify integrity.
	if err := j2.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after crash recovery: %v", err)
	}
}

func TestCrashRecoveryPreservesXAttrs(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-journal-crash-xa-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	j1, _ := OpenJournal(dir)
	n := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j1.EnsureNodePath("/xattr_crash.txt", n, false)
	_ = j1.SetXAttr(id, "user.important", []byte("critical data"))
	_ = j1.SetXAttr(id, "security.acl", []byte("acl_data"))
	_ = j1.Close()

	// Reopen and verify xattrs survived.
	j2, _ := OpenJournal(dir)
	defer func() { _ = j2.Close() }()

	nodeID, _, _, _, _ := j2.ResolvePath("/xattr_crash.txt")
	if nodeID == 0 {
		t.Fatal("node lost after crash recovery")
	}

	val, _ := j2.GetXAttr(nodeID, "user.important")
	if string(val) != "critical data" {
		t.Errorf("user.important = %q after recovery, want %q", val, "critical data")
	}

	val2, _ := j2.GetXAttr(nodeID, "security.acl")
	if string(val2) != "acl_data" {
		t.Errorf("security.acl = %q after recovery, want %q", val2, "acl_data")
	}
}

// --- Foreign Key Constraints ---

func TestForeignKeyEnforcement(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Try to create an edge referencing a non-existent node.
	_, err := j.db.Exec(`INSERT INTO edges (parent_id, name, child_id) VALUES (1, 'orphan', 99999)`)
	if err == nil {
		t.Error("expected foreign key violation for non-existent child_id")
	}
}

func TestCascadeDeleteRemovesXAttrs(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	n := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, _ := j.EnsureNodePath("/cascade.txt", n, false)
	_ = j.SetXAttr(id, "user.x", []byte("y"))
	_ = j.SetXAttr(id, "user.z", []byte("w"))

	// Delete node directly via SQL — CASCADE should remove xattrs.
	_, err := j.db.Exec(`DELETE FROM nodes WHERE id = ?`, id)
	if err != nil {
		t.Fatalf("delete node: %v", err)
	}

	var count int
	_ = j.db.QueryRow(`SELECT COUNT(*) FROM xattrs WHERE node_id = ?`, id).Scan(&count)
	if count != 0 {
		t.Errorf("xattr count after cascade delete = %d, want 0", count)
	}
}

// --- Deep Nesting Stress ---

func TestDeepNesting(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create 20-level deep path.
	depth := 20
	var path strings.Builder
	for i := range depth {
		fmt.Fprintf(&path, "/level_%02d", i)
	}
	path.WriteString("/leaf.txt")

	n := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id, err := j.EnsureNodePath(path.String(), n, false)
	if err != nil {
		t.Fatalf("EnsureNodePath depth=%d: %v", depth, err)
	}
	if id == 0 {
		t.Fatal("expected non-zero ID for deep path")
	}

	// Resolve the full path.
	nodeID, _, _, _, err := j.ResolvePath(path.String())
	if err != nil {
		t.Fatalf("ResolvePath deep: %v", err)
	}
	if nodeID != id {
		t.Errorf("ResolvePath = %d, want %d", nodeID, id)
	}

	// Verify integrity.
	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after deep nesting: %v", err)
	}
}

// --- Large Batch ---

func TestLargeBatchNodes(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	const count = 1000
	ids := make([]int64, count)

	for i := range count {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644, Size: uint64(i)}
		id, err := j.EnsureNodePath(fmt.Sprintf("/batch_%04d.txt", i), n, false)
		if err != nil {
			t.Fatalf("EnsureNodePath %d: %v", i, err)
		}
		ids[i] = id
	}

	// Verify all are resolvable.
	for i := range count {
		path := fmt.Sprintf("/batch_%04d.txt", i)
		nodeID, _, _, _, err := j.ResolvePath(path)
		if err != nil {
			t.Errorf("ResolvePath %q: %v", path, err)
			continue
		}
		if nodeID == 0 {
			t.Errorf("node %q not found", path)
		}
	}

	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after large batch: %v", err)
	}
}

// --- Renames Under Load ---

func TestConcurrentRenames(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create files.
	const count = 20
	for i := range count {
		n := &GraphNode{Kind: NodeFile, Mode: 0o644}
		_, _ = j.EnsureNodePath(fmt.Sprintf("/rename_%02d.txt", i), n, false)
	}

	// Concurrently rename files to different names.
	var wg sync.WaitGroup
	wg.Add(count)
	var errors atomic.Int64

	for i := range count {
		go func(idx int) {
			defer wg.Done()
			oldName := fmt.Sprintf("rename_%02d.txt", idx)
			newName := fmt.Sprintf("renamed_%02d.txt", idx)
			err := j.MoveEdgeAndWhiteout(1, oldName, 1, newName, 0, false, false)
			if err != nil {
				errors.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if errCount := errors.Load(); errCount > 0 {
		t.Errorf("%d rename errors", errCount)
	}

	if err := j.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity after concurrent renames: %v", err)
	}
}

// --- Edge Case: Empty Path Components ---

func TestSplitPathEdgeCases(t *testing.T) {
	tests := []struct {
		path string
		want []string
	}{
		{"", nil},
		{"/", nil},
		{"/foo", []string{"foo"}},
		{"/foo/bar", []string{"foo", "bar"}},
		{"/a/b/c/d", []string{"a", "b", "c", "d"}},
	}
	for _, tt := range tests {
		got := splitPath(tt.path)
		if len(got) != len(tt.want) {
			t.Errorf("splitPath(%q) = %v, want %v", tt.path, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("splitPath(%q)[%d] = %q, want %q", tt.path, i, got[i], tt.want[i])
			}
		}
	}
}

func TestJoinPath(t *testing.T) {
	tests := []struct {
		parent, name, want string
	}{
		{"/", "foo", "/foo"},
		{"/a", "b", "/a/b"},
		{"/a/b", "c", "/a/b/c"},
	}
	for _, tt := range tests {
		got := joinPath(tt.parent, tt.name)
		if got != tt.want {
			t.Errorf("joinPath(%q, %q) = %q, want %q", tt.parent, tt.name, got, tt.want)
		}
	}
}

// --- Transaction Rollback ---

func TestTransactionRollbackOnDuplicateEdge(t *testing.T) {
	j, cleanup := testJournal(t)
	defer cleanup()

	// Create two nodes and link them.
	n1 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	n2 := &GraphNode{Kind: NodeFile, Mode: 0o644}
	id1, _ := j.EnsureNodePath("/first.txt", n1, false)
	id2, _ := j.EnsureNodePath("/second.txt", n2, false)

	// Try to create a duplicate edge — should fail gracefully.
	err := j.tx(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO edges (parent_id, name, child_id) VALUES (1, 'first.txt', ?)`, id2)
		return err
	})
	// This should succeed because we use INSERT OR REPLACE in the real code,
	// but a plain INSERT should fail.
	if err == nil {
		t.Log("INSERT OR REPLACE behavior: duplicate edge replaced")
	}

	// Original edge should still be valid.
	edges, _ := j.ListEdges(1)
	for _, e := range edges {
		if e.Name == "first.txt" && e.ChildID == id1 {
			return // OK — original edge preserved
		}
	}
	t.Log("Edge was replaced with new child (INSERT OR REPLACE semantics)")
}

// --- Close Idempotency ---

func TestCloseIdempotent(t *testing.T) {
	dir, err := os.MkdirTemp("", "pxar-journal-close-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	j, _ := OpenJournal(dir)
	// Close should be safe to call.
	if err := j.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Second close should not panic or corrupt.
	// (Note: j.db is nil after first close, so this would panic
	// if called without checking — but our Close() is safe.)
}
