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
	ctx      context.Context
	Snapshot *snapshots.WinVSSSnapshot
	mu       sync.Mutex
}

func NewSftpHandler(ctx context.Context, snapshot *snapshots.WinVSSSnapshot) (*sftp.Handlers, error) {
	handler := &SftpHandler{ctx: ctx, Snapshot: snapshot}

	return &sftp.Handlers{
		FileGet:  handler,
		FilePut:  handler,
		FileCmd:  handler,
		FileList: handler,
	}, nil
}

func (h *SftpHandler) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	flags := r.Pflags()
	if !flags.Read {
		return nil, os.ErrInvalid
	}

	go h.Snapshot.UpdateTimestamp()
	h.setFilePath(r)

	return os.Open(r.Filepath)
}

func (h *SftpHandler) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	return nil, fmt.Errorf("unsupported file command: %s", r.Method)
}

func (h *SftpHandler) Filecmd(r *sftp.Request) error {
	return fmt.Errorf("unsupported file command: %s", r.Method)
}

func (h *SftpHandler) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	_ = r.WithContext(r.Context())

	go h.Snapshot.UpdateTimestamp()

	h.mu.Lock()
	defer h.mu.Unlock()

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
