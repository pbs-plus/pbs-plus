//go:build windows
// +build windows

package vssfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gobwas/glob"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils/pattern"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/willscott/go-nfs/file"
	"golang.org/x/sys/windows"
)

func setupTestEnvironment(t *testing.T) (string, *snapshots.WinVSSSnapshot, func()) {
	tempDir, err := os.MkdirTemp("", "vssfs-test-")
	require.NoError(t, err)

	// Create test directory structure
	dirs := []string{
		"testdata",
		"testdata/subdir",
		"testdata/excluded_dir",
	}

	files := []string{
		"testdata/regular_file.txt",
		"testdata/subdir/file_in_subdir.txt",
		"testdata/system_file.txt",
	}

	for _, dir := range dirs {
		err := os.MkdirAll(filepath.Join(tempDir, dir), 0755)
		require.NoError(t, err)
	}

	for _, file := range files {
		err := os.WriteFile(filepath.Join(tempDir, file), []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Set system attribute on test file
	systemFile := filepath.Join(tempDir, "testdata/system_file.txt")
	err = windows.SetFileAttributes(
		windows.StringToUTF16Ptr(systemFile),
		windows.FILE_ATTRIBUTE_SYSTEM,
	)
	require.NoError(t, err)

	snapshot := &snapshots.WinVSSSnapshot{
		SnapshotPath: tempDir,
	}

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return tempDir, snapshot, cleanup
}

func TestStat(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	excluded := []*pattern.GlobPattern{
		{
			Glob: glob.MustCompile("**/excluded_dir/**"),
		},
	}
	fs := NewVSSFS(snapshot, "testdata", excluded).(*VSSFS)

	t.Run("regular file", func(t *testing.T) {
		info, err := fs.Stat("regular_file.txt")
		require.NoError(t, err)
		assert.False(t, info.IsDir())
		assert.Equal(t, "regular_file.txt", info.Name())
		assert.Equal(t, int64(4), info.Size())
	})

	t.Run("directory", func(t *testing.T) {
		info, err := fs.Stat("subdir")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
		assert.Equal(t, os.ModeDir|0755, info.Mode())
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := fs.Stat("missing_file.txt")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("excluded directory", func(t *testing.T) {
		_, err := fs.Stat("excluded_dir")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("system file", func(t *testing.T) {
		_, err := fs.Stat("system_file.txt")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("root directory", func(t *testing.T) {
		info, err := fs.Stat("/")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
		assert.Equal(t, "/", info.Name())
	})

	t.Run("current directory", func(t *testing.T) {
		info, err := fs.Stat(".")
		require.NoError(t, err)
		assert.True(t, info.IsDir())
	})
}

func TestReadDir(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	excluded := []*pattern.GlobPattern{
		{
			Glob: glob.MustCompile("**/excluded_dir/**"),
		},
	}
	fs := NewVSSFS(snapshot, "testdata", excluded).(*VSSFS)

	t.Run("root directory", func(t *testing.T) {
		entries, err := fs.ReadDir("/")
		require.NoError(t, err)

		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}

		assert.Contains(t, names, "regular_file.txt")
		assert.Contains(t, names, "subdir")
		assert.NotContains(t, names, "excluded_dir")
		assert.NotContains(t, names, "system_file.txt")
	})

	t.Run("subdirectory", func(t *testing.T) {
		entries, err := fs.ReadDir("subdir")
		require.NoError(t, err)
		assert.Len(t, entries, 1)
		assert.Equal(t, "file_in_subdir.txt", entries[0].Name())
	})

	t.Run("non-existent directory", func(t *testing.T) {
		_, err := fs.ReadDir("missing_dir")
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("file instead of directory", func(t *testing.T) {
		_, err := fs.ReadDir("regular_file.txt")
		assert.ErrorContains(t, err, "not a directory")
	})
}

func TestPathNormalization(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	fs := NewVSSFS(snapshot, "testdata", nil).(*VSSFS)

	testCases := []struct {
		input    string
		expected string
	}{
		{"/", "/"},
		{".", "/"},
		{"", "/"},
		{"subdir", "/SUBDIR"},
		{"subdir/", "/SUBDIR/"},
		{"mixed\\slashes", "/MIXED/SLASHES"},
		{"./../testdata", "/TESTDATA"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := fs.normalizePath(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFileIDStability(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	fs := NewVSSFS(snapshot, "testdata", nil).(*VSSFS)

	t.Run("same path same ID", func(t *testing.T) {
		id1, err := fs.getStableID("/subdir")
		require.NoError(t, err)

		id2, err := fs.getStableID("subdir")
		require.NoError(t, err)

		assert.Equal(t, id1, id2)
	})

	t.Run("different paths different IDs", func(t *testing.T) {
		dirID, err := fs.getStableID("/subdir")
		require.NoError(t, err)

		fileID, err := fs.getStableID("/subdir/file_in_subdir.txt")
		require.NoError(t, err)

		assert.NotEqual(t, dirID, fileID)
	})
}

func TestSyscallCount(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	fs := NewVSSFS(snapshot, "testdata", nil).(*VSSFS)

	t.Run("directory listing", func(t *testing.T) {
		start := time.Now()
		_, err := fs.ReadDir("subdir")
		require.NoError(t, err)
		elapsed := time.Since(start)

		// Should complete in <1ms if using efficient syscalls
		assert.True(t, elapsed < time.Millisecond,
			"ReadDir took too long: %v", elapsed)
	})

	t.Run("stat operations", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 100; i++ {
			_, err := fs.Stat("regular_file.txt")
			require.NoError(t, err)
		}
		elapsed := time.Since(start)

		// Should complete 100 stats in <10ms
		assert.True(t, elapsed < 10*time.Millisecond,
			"Stat operations took too long: %v", elapsed)
	})
}

func TestNFSMetadata(t *testing.T) {
	_, snapshot, cleanup := setupTestEnvironment(t)
	defer cleanup()

	fs := NewVSSFS(snapshot, "testdata", nil).(*VSSFS)

	t.Run("file metadata", func(t *testing.T) {
		info, err := fs.Stat("regular_file.txt")
		require.NoError(t, err)

		vssInfo := info.(*VSSFileInfo)
		sys := vssInfo.Sys().(file.FileInfo)

		assert.NotZero(t, sys.Fileid)
		assert.Equal(t, uint32(1), sys.Nlink)
	})

	t.Run("directory metadata", func(t *testing.T) {
		info, err := fs.Stat("subdir")
		require.NoError(t, err)

		vssInfo := info.(*VSSFileInfo)
		sys := vssInfo.Sys().(file.FileInfo)

		t.Logf("Info: %+v", vssInfo)

		assert.NotZero(t, sys.Fileid)
		assert.Equal(t, uint32(2), sys.Nlink)
	})
}
