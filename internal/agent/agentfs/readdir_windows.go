//go:build windows

package agentfs

import (
	"os"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
)

func buildFileInfo(entry os.FileInfo, blockSize uint64) types.AgentFileInfo {
	sys := entry.Sys().(*syscall.Win32FileAttributeData)

	if sys.FileAttributes&excludedAttrs != 0 {
		return types.AgentFileInfo{}
	}

	fileSize := int64(sys.FileSizeHigh)<<32 | int64(sys.FileSizeLow)

	info := types.AgentFileInfo{
		Name:           entry.Name(),
		Mode:           uint32(entry.Mode()),
		IsDir:          entry.IsDir(),
		Size:           fileSize,
		ModTime:        entry.ModTime().UnixNano(),
		CreationTime:   filetimeSyscallToUnix(sys.CreationTime),
		LastAccessTime: filetimeSyscallToUnix(sys.LastAccessTime),
		LastWriteTime:  filetimeSyscallToUnix(sys.LastWriteTime),
		FileAttributes: parseFileAttributes(sys.FileAttributes),
	}

	if !info.IsDir && fileSize > 0 {
		info.Blocks = uint64((fileSize + int64(blockSize) - 1) / int64(blockSize))
	}

	return info
}
