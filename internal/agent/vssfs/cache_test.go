package vssfs

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestIsPrefix(t *testing.T) {
	tests := []struct {
		p        string
		child    string
		expected bool
	}{
		{"/a", "/a", true},
		{"/a", "/a/b", true},
		{"/a", "/ab", false},
		{"/a/b", "/a/b/c", true},
		{"/a/b", "/a/b", true},
		{"/a/b", "/a/bc", false},
		{"/a", "/a/b/c/d", true},
		{"/a/b", "/a", false},
	}

	for _, tt := range tests {
		result := isPrefix(tt.p, tt.child)
		if result != tt.expected {
			t.Errorf("isPrefix(%q, %q) = %v, expected %v", tt.p, tt.child, result, tt.expected)
		}
	}
}

// -----------------------
// Tests for DFSCache
// -----------------------

// TestDFSCacheBasicLookup verifies that when a single directory is cached,
// a lookup for a file in that directory is successful.
func TestDFSCacheBasicLookup(t *testing.T) {
	cache := NewDFSCache()

	entryA := dirCacheEntry{
		dirPath: filepath.Clean("/a"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "file1.txt"},
		},
	}

	if err := cache.PushDir(entryA); err != nil {
		t.Fatalf("PushDir failed: %v", err)
	}

	fullPath := filepath.Join("/a", "file1.txt")
	info, found := cache.Lookup("/a", fullPath)
	if !found {
		t.Fatalf("Expected to find %q in DFSCache", fullPath)
	}
	if info.Name != "file1.txt" {
		t.Errorf("Expected file1.txt, got %q", info.Name)
	}
}

// TestDFSCacheInvalidation simulates a DFS branch that later changes branch.
func TestDFSCacheInvalidation(t *testing.T) {
	cache := NewDFSCache()

	// Simulate the branch:
	//   /a      -> fileA.txt
	//   /a/b    -> fileB.txt
	//   /a/b/c  -> fileC.txt
	entryA := dirCacheEntry{
		dirPath: filepath.Clean("/a"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileA.txt"},
		},
	}
	entryB := dirCacheEntry{
		dirPath: filepath.Clean("/a/b"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileB.txt"},
		},
	}
	entryC := dirCacheEntry{
		dirPath: filepath.Clean("/a/b/c"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileC.txt"},
		},
	}

	if err := cache.PushDir(entryA); err != nil {
		t.Fatalf("PushDir entryA failed: %v", err)
	}
	if err := cache.PushDir(entryB); err != nil {
		t.Fatalf("PushDir entryB failed: %v", err)
	}
	if err := cache.PushDir(entryC); err != nil {
		t.Fatalf("PushDir entryC failed: %v", err)
	}

	if len(cache.stack) != 3 {
		t.Fatalf("Expected stack length 3, got %d", len(cache.stack))
	}

	// Change branch: active directory becomes "/a/b/d",
	// so "/a/b/c" (and any deeper than "/a/b") should be invalidated.
	cache.invalidateForPath(filepath.Clean("/a/b/d"))

	if len(cache.stack) != 2 {
		t.Errorf("After invalidation, expected stack length 2, got %d", len(cache.stack))
	}

	lookupPathB := filepath.Join("/a/b", "fileB.txt")
	info, found := cache.Lookup("/a/b/d", lookupPathB)
	if !found {
		t.Errorf("Expected to find %q after invalidation", lookupPathB)
	}
	if info.Name != "fileB.txt" {
		t.Errorf("Expected fileB.txt, got %q", info.Name)
	}

	lookupPathC := filepath.Join("/a/b/c", "fileC.txt")
	_, found = cache.Lookup("/a/b/d", lookupPathC)
	if found {
		t.Errorf("Did not expect to find %q after invalidation", lookupPathC)
	}
}

// TestDFSCacheFullInvalidation simulates an active path that is completely outside
// the current branch so that the entire cache is invalidated.
func TestDFSCacheFullInvalidation(t *testing.T) {
	cache := NewDFSCache()

	entryA := dirCacheEntry{
		dirPath: filepath.Clean("/a"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileA.txt"},
		},
	}
	entryB := dirCacheEntry{
		dirPath: filepath.Clean("/a/b"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileB.txt"},
		},
	}

	if err := cache.PushDir(entryA); err != nil {
		t.Fatalf("PushDir entryA failed: %v", err)
	}
	if err := cache.PushDir(entryB); err != nil {
		t.Fatalf("PushDir entryB failed: %v", err)
	}

	// Active path is outside the cached branch.
	cache.invalidateForPath(filepath.Clean("/x/y"))

	if len(cache.stack) != 0 {
		t.Errorf("Expected stack length 0 after full invalidation, got %d", len(cache.stack))
	}
}

// TestDFSCacheNoInvalidation ensures that if activePath is still within a cached branch,
// nothing is invalidated.
func TestDFSCacheNoInvalidation(t *testing.T) {
	cache := NewDFSCache()

	entryA := dirCacheEntry{
		dirPath: filepath.Clean("/a"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileA.txt"},
		},
	}

	if err := cache.PushDir(entryA); err != nil {
		t.Fatalf("PushDir entryA failed: %v", err)
	}

	// Active path is within "/a" (e.g. "/a/b").
	cache.invalidateForPath(filepath.Clean("/a/b"))

	if len(cache.stack) != 1 {
		t.Errorf("Expected stack length 1, got %d", len(cache.stack))
	}

	lookupPath := filepath.Join("/a", "fileA.txt")
	info, found := cache.Lookup("/a/b", lookupPath)
	if !found {
		t.Errorf("Expected to find %q in DFSCache", lookupPath)
	}
	if info.Name != "fileA.txt" {
		t.Errorf("Expected fileA.txt, got %q", info.Name)
	}
}

// TestDFSCacheDeepTraversal simulates a deep DFS branch and then a partial invalidation.
func TestDFSCacheDeepTraversal(t *testing.T) {
	cache := NewDFSCache()
	// Build a deep branch.
	paths := []string{"/a", "/a/b", "/a/b/c", "/a/b/c/d", "/a/b/c/d/e"}
	for _, p := range paths {
		entry := dirCacheEntry{
			dirPath: filepath.Clean(p),
			entries: ReadDirEntries{
				&VSSFileInfo{Name: "file" + strings.ToLower(filepath.Base(p)) + ".txt"},
			},
		}
		if err := cache.PushDir(entry); err != nil {
			t.Fatalf("PushDir failed for %q: %v", p, err)
		}
	}
	if len(cache.stack) != len(paths) {
		t.Fatalf("Expected stack length %d, got %d", len(paths), len(cache.stack))
	}

	// Use an active path in the middle of the branch.
	activePath := filepath.Clean("/a/b/c/x")
	cache.invalidateForPath(activePath)

	// The valid branch now should only include directories up to "/a/b/c".
	expectedStack := []string{"/a", "/a/b", "/a/b/c"}
	if len(cache.stack) != len(expectedStack) {
		t.Fatalf("After invalidation expected stack length %d, got %d", len(expectedStack), len(cache.stack))
	}
	for i, entry := range cache.stack {
		if entry.dirPath != filepath.Clean(expectedStack[i]) {
			t.Errorf("At index %d, expected dir %q, got %q", i, expectedStack[i], entry.dirPath)
		}
	}

	lookupPath := filepath.Join("/a/b/c", "filec.txt")
	info, found := cache.Lookup(activePath, lookupPath)
	if !found {
		t.Errorf("Expected to find %q in DFSCache", lookupPath)
	}
	if info.Name != "filec.txt" {
		t.Errorf("Expected filec.txt, got %q", info.Name)
	}

	lookupPathDeep := filepath.Join("/a/b/c/d/e", "filee.txt")
	_, found = cache.Lookup(activePath, lookupPathDeep)
	if found {
		t.Errorf("Did not expect to find %q after invalidation", lookupPathDeep)
	}
}

// TestDFSCacheMixedBranches simulates sibling branches.
func TestDFSCacheMixedBranches(t *testing.T) {
	cache := NewDFSCache()

	// Create two sibling entries under "/a":
	entryB := dirCacheEntry{
		dirPath: filepath.Clean("/a/b"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "fileb.txt"},
		},
	}
	entryC := dirCacheEntry{
		dirPath: filepath.Clean("/a/c"),
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "filec.txt"},
		},
	}
	// Push /a/b then /a/c.
	if err := cache.PushDir(entryB); err != nil {
		t.Fatalf("PushDir entryB failed: %v", err)
	}
	if err := cache.PushDir(entryC); err != nil {
		t.Fatalf("PushDir entryC failed: %v", err)
	}

	// Simulate changing branch.
	cache.invalidateForPath(filepath.Clean("/a/d/e"))
	// Neither /a/b nor /a/c is an ancestor of "/a/d/e", so the cache should empty.
	if len(cache.stack) != 0 {
		t.Errorf("Expected full cache invalidation, but stack length is %d", len(cache.stack))
	}
}

// TestDFSCacheRelativePaths tests handling of trailing slashes and relative path variations.
func TestDFSCacheRelativePaths(t *testing.T) {
	cache := NewDFSCache()

	entryA := dirCacheEntry{
		dirPath: "/a", // no trailing slash.
		entries: ReadDirEntries{
			&VSSFileInfo{Name: "file.txt"},
		},
	}
	if err := cache.PushDir(entryA); err != nil {
		t.Fatalf("PushDir entryA failed: %v", err)
	}

	lookupPath := filepath.Join("/a/", "file.txt")
	info, found := cache.Lookup("/a/", lookupPath)
	if !found {
		t.Fatalf("Expected to find %q in DFSCache", lookupPath)
	}
	if info.Name != "file.txt" {
		t.Errorf("Expected file.txt, got %q", info.Name)
	}
}

// TestDFSCacheLookupEmptyStack ensures that a lookup returns false when no directories are cached.
func TestDFSCacheLookupEmptyStack(t *testing.T) {
	cache := NewDFSCache()
	_, found := cache.Lookup("/a", "/a/file.txt")
	if found {
		t.Errorf("Expected lookup in empty DFSCache to not find any entry")
	}
}
