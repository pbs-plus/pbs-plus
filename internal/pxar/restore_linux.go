//go:build linux

package pxar

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func localRestoreDir(pr *PxarReader, dst string, dirEntry *EntryInfo) error {
	entries, err := pr.ReadDir(dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())
		switch e.FileType {
		case FileTypeDirectory:
			if err := os.MkdirAll(target, os.FileMode(e.Mode&0777)); err != nil {
				return fmt.Errorf("mkdir %q: %w", target, err)
			}
			if err := localRestoreDir(pr, target, &e); err != nil {
				return err
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				return err
			}
		case FileTypeFile:
			if err := localRestoreFile(pr, target, &e); err != nil {
				return err
			}
		case FileTypeSymlink:
			if err := localRestoreSymlink(pr, target, &e); err != nil {
				return err
			}
		case FileTypeFifo:
			if err := syscall.Mkfifo(target, uint32(e.Mode&0777)); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mkfifo %q: %w", target, err)
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				return err
			}
		case FileTypeSocket:
			if err := syscall.Mknod(target, syscall.S_IFSOCK|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mksocket %q: %w", target, err)
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				return err
			}
		case FileTypeDevice:
			if err := syscall.Mknod(target, syscall.S_IFCHR|uint32(e.Mode&0777), 0); err != nil && !os.IsExist(err) {
				return fmt.Errorf("mknod %q: %w", target, err)
			}
			if err := localApplyMeta(pr, target, &e); err != nil {
				return err
			}
		case FileTypeHardlink:
		}
	}
	return nil
}
