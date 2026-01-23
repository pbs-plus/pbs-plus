//go:build linux

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func localRestoreDir(
	ctx context.Context,
	pr *PxarReader,
	dst string,
	dirEntry *EntryInfo,
) error {
	entries, err := pr.ReadDir(dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		target := filepath.Join(dst, e.Name())
		switch e.FileType {
		case FileTypeDirectory:
			if err := os.MkdirAll(target, os.FileMode(e.Mode&0777)); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mkdir").Write()
				continue
			}
			// Pass context into recursive call
			if err := localRestoreDir(ctx, pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "dir").Write()
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "meta").Write()
				continue
			}
		case FileTypeFile:
			if err := localRestoreFile(ctx, pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "file").Write()
				continue
			}
		case FileTypeSymlink:
			if err := localRestoreSymlink(ctx, pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "symlink").Write()
				continue
			}
		case FileTypeFifo:
			if err := syscall.Mkfifo(target, uint32(e.Mode&0777)); err != nil &&
				!os.IsExist(err) {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mkfifo").Write()
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mkfifo").Write()
				continue
			}
		case FileTypeSocket:
			if err := syscall.Mknod(
				target,
				syscall.S_IFSOCK|uint32(e.Mode&0777),
				0,
			); err != nil && !os.IsExist(err) {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mknod").Write()
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mknod").Write()
				continue
			}
		case FileTypeDevice:
			if err := syscall.Mknod(
				target,
				syscall.S_IFCHR|uint32(e.Mode&0777),
				0,
			); err != nil && !os.IsExist(err) {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mknod").Write()
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				syslog.L.Error(err).WithField("restore", dst).WithField("op", "mknod").Write()
				continue
			}
		case FileTypeHardlink:
			// Implementation for hardlinks would go here if applicable
		}
	}
	return nil
}
