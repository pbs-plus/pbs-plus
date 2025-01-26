//go:build windows

package vssfs

import (
	"os"

	"github.com/willscott/go-nfs/file"
)

type VSSFileInfo struct {
	os.FileInfo
	stableID uint64
}

func (vi *VSSFileInfo) Sys() interface{} {
	return file.FileInfo{
		Nlink:  1,
		UID:    1000,
		GID:    1000,
		Major:  0,
		Minor:  0,
		Fileid: vi.stableID,
	}
}
