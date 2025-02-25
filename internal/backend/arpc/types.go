package arpcfs

import (
	"context"
	"os"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc"
)

// ARPCFS implements billy.Filesystem using aRPC calls
type ARPCFS struct {
	ctx     context.Context
	session *arpc.Session
	drive   string
}

// ARPCFile implements billy.File for remote files
type ARPCFile struct {
	fs       *ARPCFS
	name     string
	offset   int64
	handleID string
	isClosed bool
	drive    string
}

// FileInfoResponse represents server's file info response
type FileInfoResponse struct {
	Name    string      `json:"name"`
	Size    int64       `json:"size"`
	Mode    os.FileMode `json:"mode"`
	ModTime time.Time   `json:"modTime"`
	IsDir   bool        `json:"isDir"`
}

// ReadDirResponse represents server's directory listing
type ReadDirResponse struct {
	Entries []FileInfoResponse `json:"entries"`
}

// OpenRequest represents OpenFile request payload
type OpenRequest struct {
	Path string `json:"path"`
	Flag int    `json:"flag"`
	Perm int    `json:"perm"`
}

// ReadRequest represents Read request payload
type ReadRequest struct {
	HandleID string `json:"handleID"`
	Offset   int64  `json:"offset"`
	Length   int    `json:"length"`
}

// ReadResponse represents Read response
type ReadResponse struct {
	Data []byte `json:"data"`
	EOF  bool   `json:"eof"`
}
