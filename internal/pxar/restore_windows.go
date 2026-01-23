//go:build windows

package pxar

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mkdir").Write()
				continue
			}
			if err := remoteRestoreDir(ctx, client, target, e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "dir").Write()
				continue
			}
			if err := remoteApplyMeta(ctx, client, target, e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "meta").Write()
				continue
			}
		case FileTypeFile:
			if err := remoteRestoreFile(ctx, client, target, e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "file").Write()
				continue
			}
		case FileTypeSymlink:
			if err := remoteRestoreSymlink(ctx, client, target, e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "symlink").Write()
				continue
			}
		case FileTypeFifo:
		case FileTypeSocket:
		case FileTypeDevice:
		case FileTypeHardlink:
		}
	}
	return nil
}
