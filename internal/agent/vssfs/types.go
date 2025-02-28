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
	HandleID int `msg:"handleID"`
	Length   int `msg:"length"`
}

type ReadAtReq struct {
	HandleID int   `msg:"handleID"`
	Offset   int64 `msg:"offset"`
	Length   int   `msg:"length"`
}

type CloseReq struct {
	HandleID int `msg:"handleID"`
}

type FstatReq struct {
	HandleID int `msg:"handleID"`
}

type BackupReq struct {
	JobId string `msg:"job_id"`
	Drive string `msg:"drive"`
}

type VSSFileInfo struct {
	Name    string    `msg:"name"`
	Size    int64     `msg:"size"`
	Mode    uint32    `msg:"mode"`
	ModTime time.Time `msg:"modTime"`
	IsDir   bool      `msg:"isDir"`
}

type ReadDirEntries []*VSSFileInfo
type FileHandleId int

type DataResponse struct {
	Data []byte `msg:"data"`
	EOF  bool   `msg:"eof"`
}

type FSStat struct {
	TotalSize      int64         `msg:"total_size"`
	FreeSize       int64         `msg:"free_size"`
	AvailableSize  int64         `msg:"available_size"`
	TotalFiles     int           `msg:"total_files"`
	FreeFiles      int           `msg:"free_files"`
	AvailableFiles int           `msg:"available_files"`
	CacheHint      time.Duration `msg:"cache_hint"`
}
