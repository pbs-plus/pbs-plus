//go:build windows

package vssfs

import (
	"io/fs"
)

type VSSFileInfo struct {
	StableID uint64      `json:"id"`
	Name     string      `json:"name"`
	Size     int64       `json:"size"`
	Mode     fs.FileMode `json:"mode"`
	ModTime  int64       `json:"modTime"`
	IsDir    bool        `json:"isDir"`
}
