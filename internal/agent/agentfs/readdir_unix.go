//go:build unix

package agentfs

import (
	"errors"
	"os"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

func readDirBulk(dirPath string) ([]byte, error) {
	// Open the directory
	dir, err := os.Open(dirPath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

	// Read all directory entries
	entries, err := dir.Readdir(-1)
	if err != nil {
		return nil, err
	}

	var resultEntries types.ReadDirEntries

	for _, entry := range entries {
		// Skip "." and ".."
		if entry.Name() == "." || entry.Name() == ".." {
			continue
		}

		// Get file attributes
		stat, ok := entry.Sys().(*syscall.Stat_t)
		if !ok {
			return nil, errors.New("failed to retrieve file attributes")
		}

		// Filter out specific attributes (e.g., symlinks, devices, etc.)
		if (stat.Mode&syscall.S_IFMT) == syscall.S_IFLNK || // Symlink
			(stat.Mode&syscall.S_IFMT) == syscall.S_IFCHR || // Character device
			(stat.Mode&syscall.S_IFMT) == syscall.S_IFBLK || // Block device
			(stat.Mode&syscall.S_IFMT) == syscall.S_IFIFO || // FIFO
			(stat.Mode&syscall.S_IFMT) == syscall.S_IFSOCK { // Socket
			continue
		}

		// Convert file mode to os.FileMode
		mode := entry.Mode()

		// Append the entry to the result
		resultEntries = append(resultEntries, types.AgentDirEntry{
			Name: []byte(entry.Name()),
			Mode: uint32(mode),
		})
	}

	// Encode the result entries
	return resultEntries.Encode()
}
