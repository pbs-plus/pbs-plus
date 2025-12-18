//go:build unix

package pxar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func restoreDir(ctx context.Context, client *RemoteClient, dst string, dirEntry EntryInfo) error {
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
				return fmt.Errorf("mkdir %q: %w", target, err)
			}
			if err := restoreDir(ctx, client, target, e); err != nil {
				return err
			}
			if err := applyMeta(target, e); err != nil {
				return err
			}
		case FileTypeFile:
			if err := restoreFile(ctx, client, target, e); err != nil {
				return err
			}
		case FileTypeSymlink:
			if err := restoreSymlink(ctx, client, target, e); err != nil {
				return err
			}
		case FileTypeFifo:
			if err := syscall.Mkfifo(target, uint32(e.Mode&0777)); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mkfifo %q: %w", target, err)
			}
		case FileTypeSocket:
			if err := syscall.Mknod(target, syscall.S_IFSOCK|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mksocket %q: %w", target, err)
			}
		case FileTypeDevice:
			if err := syscall.Mknod(target, syscall.S_IFCHR|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mknod %q: %w", target, err)
			}
		case FileTypeHardlink:
		}
	}
	return nil
}
