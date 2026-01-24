//go:build linux

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
)

func localRestoreDir(
	ctx context.Context,
	pr *PxarReader,
	dst string,
	dirEntry *EntryInfo,
	errCh chan error,
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
				errCh <- err
				continue
			}
			if err := localRestoreDir(ctx, pr, target, &e, errCh); err != nil {
				errCh <- err
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeFile:
			if err := localRestoreFile(ctx, pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeSymlink:
			if err := localRestoreSymlink(ctx, pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeFifo:
			if err := syscall.Mkfifo(target, uint32(e.Mode&0777)); err != nil &&
				!os.IsExist(err) {
				errCh <- err
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeSocket:
			if err := syscall.Mknod(
				target,
				syscall.S_IFSOCK|uint32(e.Mode&0777),
				0,
			); err != nil && !os.IsExist(err) {
				errCh <- err
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeDevice:
			if err := syscall.Mknod(
				target,
				syscall.S_IFCHR|uint32(e.Mode&0777),
				0,
			); err != nil && !os.IsExist(err) {
				errCh <- err
				continue
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				errCh <- err
				continue
			}
		case FileTypeHardlink:
			// Implementation for hardlinks would go here if applicable
		}
	}

	return nil
}
