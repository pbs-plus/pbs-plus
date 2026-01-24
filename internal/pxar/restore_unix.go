//go:build unix

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

func remoteRestoreDir(ctx context.Context, client *RemoteClient, dst string, dirEntry EntryInfo, jobs chan<- restoreJob, wg *sync.WaitGroup) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := client.ReadDir(ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())

		switch e.FileType {
		case FileTypeDirectory, FileTypeFile, FileTypeSymlink:
			wg.Add(1)
			job := restoreJob{dest: target, info: e}
			select {
			case jobs <- job:
			case <-ctx.Done():
				wg.Done()
				return ctx.Err()
			}

		case FileTypeFifo, FileTypeSocket:
			var opErr error
			mode := uint32(e.Mode & 0777)
			if e.FileType == FileTypeFifo {
				opErr = syscall.Mkfifo(target, mode)
			} else {
				opErr = syscall.Mknod(target, syscall.S_IFSOCK|mode, 0)
			}

			if opErr == nil || os.IsExist(opErr) {
				if f, openErr := os.OpenFile(target, os.O_RDONLY, 0); openErr == nil {
					_ = remoteApplyMeta(ctx, client, f, e)
				}
			}
		}
	}

	df, err := os.Open(dst)
	if err != nil {
		return err
	}
	return remoteApplyMeta(ctx, client, df, dirEntry)
}
