//go:build windows
// +build windows

package sftp

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/pkg/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent/snapshots"
)

type SftpHandler struct {
	ctx         context.Context
	mu          sync.Mutex
	DriveLetter string
	Snapshot    *snapshots.WinVSSSnapshot
}

func NewSftpHandler(ctx context.Context, driveLetter string, snapshot *snapshots.WinVSSSnapshot) (*sftp.Handlers, error) {
	handler := &SftpHandler{ctx: ctx, DriveLetter: driveLetter, Snapshot: snapshot}

	return &sftp.Handlers{
		FileGet:  handler,
		FilePut:  handler,
		FileCmd:  handler,
		FileList: handler,
	}, nil
}

func (h *SftpHandler) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Snapshot.UpdateTimestamp()
	h.setFilePath(r)

	file, err := h.fetch(r.Filepath, os.O_RDONLY)
	if err != nil {
		log.Printf("error reading file: %v", err)
		return nil, err
	}
	return file, nil
}

func (h *SftpHandler) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	return nil, fmt.Errorf("unsupported file command: %s", r.Method)
}

func (h *SftpHandler) Filecmd(r *sftp.Request) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return fmt.Errorf("unsupported file command: %s", r.Method)
}

func (h *SftpHandler) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.Snapshot.UpdateTimestamp()
	h.setFilePath(r)

	switch r.Method {
	case "List":
		list, err := h.FileLister(r.Filepath)
		if err != nil {
			log.Printf("error listing files: %v", err)
			return nil, err
		}
		return list, nil
	case "Stat":
		stats, err := h.FileStat(r.Filepath)
		if err != nil {
			log.Printf("error getting file stats: %v", err)
			return nil, err
		}
		return stats, nil
	default:
		return nil, fmt.Errorf("unsupported file list command: %s", r.Method)
	}
}
