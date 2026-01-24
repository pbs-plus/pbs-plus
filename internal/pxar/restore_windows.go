//go:build windows

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/windows"
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
		wg.Add(1)
		job := restoreJob{dest: target, info: e}
		select {
		case jobs <- job:
		case <-ctx.Done():
			wg.Done()
			return ctx.Err()
		}
	}

	pathPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return err
	}

	h, err := windows.CreateFile(
		pathPtr,
		windows.FILE_WRITE_ATTRIBUTES|windows.WRITE_DAC|windows.WRITE_OWNER,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return err
	}

	df := os.NewFile(uintptr(h), dst)
	return remoteApplyMeta(ctx, client, df, dirEntry)
}
