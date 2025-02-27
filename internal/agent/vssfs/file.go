//go:build windows

package vssfs

import (
	"io/fs"
)

type VSSFileInfo struct {
	Name    string      `msgpack:"name"`
	Size    int64       `msgpack:"size"`
	Mode    fs.FileMode `msgpack:"mode"`
	ModTime int64       `msgpack:"modTime"`
	IsDir   bool        `msgpack:"isDir"`
}
