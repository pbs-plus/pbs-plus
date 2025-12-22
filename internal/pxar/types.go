package pxar

import (
	"net"
	"os/exec"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

type Request struct {
	_msgpack struct{} `cbor:",toarray"`
	Variant  string
	Data     any
}

type Response map[string]any

type PxarReader struct {
	conn net.Conn
	mu   sync.Mutex
	enc  cbor.EncMode
	dec  cbor.DecMode
	cmd  *exec.Cmd

	FileCount   int64
	FolderCount int64
	TotalBytes  int64

	lastAccessTime  int64
	lastBytesTime   int64
	lastFileCount   int64
	lastFolderCount int64
	lastTotalBytes  int64
}

type PxarReaderStats struct {
	ByteReadSpeed   float64
	FileAccessSpeed float64
	FilesAccessed   int64
	FoldersAccessed int64
	TotalAccessed   int64
	TotalBytes      uint64
	StatCacheHits   int64
}
