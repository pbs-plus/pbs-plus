//go:generate msgp

package vssfs

import (
	"time"
)

type OpenFileReq struct {
	Path string `msg:"path"`
	Flag int    `msg:"flag"`
	Perm int    `msg:"perm"`
}

type StatReq struct {
	Path string `msg:"path"`
}

type ReadDirReq struct {
	Path string `msg:"path"`
}

type ReadReq struct {
	HandleID FileHandleId `msg:"handleID"`
	Length   int          `msg:"length"`
}

type ReadAtReq struct {
	HandleID FileHandleId `msg:"handleID"`
	Offset   int64        `msg:"offset"`
	Length   int          `msg:"length"`
}

type CloseReq struct {
	HandleID FileHandleId `msg:"handleID"`
}

type BackupReq struct {
	JobId string `msg:"job_id"`
	Drive string `msg:"drive"`
}

type LseekReq struct {
	HandleID FileHandleId `msg:"handle_id"` // File handle ID
	Offset   int64        `msg:"offset"`    // Current offset
	Whence   int          `msg:"whence"`    // SEEK_HOLE or SEEK_DATA
}

type LseekResp struct {
	NewOffset int64 `msg:"new_offset"` // New offset after seeking
}

type VSSFileInfo struct {
	Name    string    `msg:"name"`
	Size    int64     `msg:"size"`
	Mode    uint32    `msg:"mode"`
	ModTime time.Time `msg:"modTime"`
	IsDir   bool      `msg:"isDir"`
	Blocks  uint64    `msg:"blocks"`
}

type VSSDirEntry struct {
	Name string `msg:"name"`
	Mode uint32 `msg:"mode"`
}

type ReadDirEntries []*VSSDirEntry
type FileHandleId string

type StatFS struct {
	Bsize   uint64 // Block size
	Blocks  uint64 // Total blocks
	Bfree   uint64 // Free blocks
	Bavail  uint64 // Available blocks
	Files   uint64 // Total files/inodes
	Ffree   uint64 // Free files/inodes
	NameLen uint64 // Maximum name length
}
