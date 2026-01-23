//go:build unix

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
)

func remoteRestoreDir(ctx context.Context, client *RemoteClient, dst string, dirEntry EntryInfo) error {
	entries, err := client.ReadDir(ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		target := filepath.Join(dst, e.Name())
		switch e.FileType {
		case FileTypeDirectory:
			if err := os.MkdirAll(target, os.FileMode(e.Mode&0777)); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
			if err := remoteRestoreDir(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
			if err := remoteApplyMeta(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeFile:
			if err := remoteRestoreFile(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeSymlink:
			if err := remoteRestoreSymlink(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeFifo:
			if err := syscall.Mkfifo(target, uint32(e.Mode&0777)); err != nil && !os.IsExist(err) {
				_ = client.SendError(ctx, err)
				continue
			}
			if err := remoteApplyMeta(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeSocket:
			if err := syscall.Mknod(target, syscall.S_IFSOCK|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				_ = client.SendError(ctx, err)
				continue
			}
			if err := remoteApplyMeta(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeDevice:
			if err := syscall.Mknod(target, syscall.S_IFCHR|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				_ = client.SendError(ctx, err)
				continue
			}
			if err := remoteApplyMeta(ctx, client, target, e); err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		case FileTypeHardlink:
		}
	}
	return nil
}
