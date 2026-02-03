//go:build !windows

package agentfs

import (
	"os"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

func buildFileInfo(entry os.FileInfo, blockSize uint64) types.AgentFileInfo {
	info := types.AgentFileInfo{
		Name:    entry.Name(),
		Mode:    uint32(entry.Mode()),
		IsDir:   entry.IsDir(),
		Size:    entry.Size(),
		ModTime: entry.ModTime().UnixNano(),
	}

	if stat, ok := entry.Sys().(*unix.Stat_t); ok {
		info.CreationTime = int64(stat.Ctim.Sec)
		info.LastAccessTime = int64(stat.Atim.Sec)
		info.LastWriteTime = int64(stat.Mtim.Sec)

		if !info.IsDir && info.Size > 0 {
			info.Blocks = uint64(stat.Blocks)
		}
	} else {
		modTime := entry.ModTime().Unix()
		info.CreationTime = modTime
		info.LastAccessTime = modTime
		info.LastWriteTime = modTime

		if !info.IsDir && info.Size > 0 {
			info.Blocks = uint64((info.Size + int64(blockSize) - 1) / int64(blockSize))
		}
	}

	info.FileAttributes = make(map[string]bool)

	return info
}
