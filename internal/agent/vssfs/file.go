//go:build windows

package vssfs

import (
	"io/fs"
	"time"
)

type VSSFileInfo struct {
	Name    string      `msgpack:"name"`
	Size    int64       `msgpack:"size"`
	Mode    fs.FileMode `msgpack:"mode"`
	ModTime time.Time   `msgpack:"modTime"`
	IsDir   bool        `msgpack:"isDir"`
}
