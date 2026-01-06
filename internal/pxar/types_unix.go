//go:build unix

package pxar

import (
	"net"
	"os/exec"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
)

type PxarReader struct {
	conn net.Conn
	mu   sync.Mutex
	enc  cbor.EncMode
	dec  cbor.DecMode
	cmd  *exec.Cmd
	task *proxmox.RestoreTask

	FileCount   int64
	FolderCount int64
	TotalBytes  int64

	lastAccessTime  int64
	lastBytesTime   int64
	lastFileCount   int64
	lastFolderCount int64
	lastTotalBytes  int64
}

