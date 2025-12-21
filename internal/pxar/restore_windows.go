//go:build windows

package pxar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
				return fmt.Errorf("mkdir %q: %w", target, err)
			}
			if err := remoteRestoreDir(ctx, client, target, e); err != nil {
				return err
			}
			if err := remoteApplyMeta(target, e); err != nil {
				return err
			}
		case FileTypeFile:
			if err := remoteRestoreFile(ctx, client, target, e); err != nil {
				return err
			}
		case FileTypeSymlink:
			if err := remoteRestoreSymlink(ctx, client, target, e); err != nil {
				return err
			}
		case FileTypeFifo:
		case FileTypeSocket:
		case FileTypeDevice:
		case FileTypeHardlink:
		}
	}
	return nil
}
