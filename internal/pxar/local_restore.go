//go:build unix

package pxar

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

func LocalRestore(pr *PxarReader, sourceDirs []string, destDir string) []error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return []error{fmt.Errorf("mkdir root: %w", err)}
	}

	errors := make([]error, 0, len(sourceDirs))
	for _, source := range sourceDirs {
		sourceAttr, err := pr.LookupByPath(source)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if sourceAttr.IsDir() {
			err = localRestoreDir(pr, destDir, sourceAttr)
			if err != nil {
				errors = append(errors, err)
				continue
			}
		} else {
			path := filepath.Join(destDir, sourceAttr.Name())
			err = localRestoreFile(pr, path, sourceAttr)
			if err != nil {
				errors = append(errors, err)
				continue
			}
		}
	}

	return errors
}

func localRestoreFile(pr *PxarReader, path string, e *EntryInfo) error {
	mode := os.FileMode(e.Mode & 0777)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer f.Close()

	if e.ContentRange != nil {
		start, end := e.ContentRange[0], e.ContentRange[1]
		var off uint64
		buf := make([]byte, 1<<20)

		for off < e.Size {
			size := uint(len(buf))
			remain := e.Size - off
			if remain < uint64(size) {
				size = uint(remain)
			}
			data, err := pr.Read(start, end, off, size)
			if err != nil {
				return fmt.Errorf("read content %q: %w", path, err)
			}
			if len(data) == 0 {
				break
			}
			if _, err := f.Write(data); err != nil {
				return fmt.Errorf("write content %q: %w", path, err)
			}
			off += uint64(len(data))
		}
	}

	return localApplyMeta(pr, path, e)
}

func localRestoreSymlink(pr *PxarReader, path string, e *EntryInfo) error {
	target, err := pr.ReadLink(e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}
	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}
	return localApplyMetaSymlink(pr, path, e)
}

func localApplyMeta(pr *PxarReader, path string, e *EntryInfo) error {
	_ = os.Chmod(path, os.FileMode(e.Mode&0777))
	_ = os.Chown(path, int(e.UID), int(e.GID))
	mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	_ = os.Chtimes(path, mt, mt)

	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		for name, value := range xattrs {
			_ = unix.Setxattr(path, name, value, 0)
		}
	}

	return nil
}

func localApplyMetaSymlink(pr *PxarReader, path string, e *EntryInfo) error {
	_ = os.Lchown(path, int(e.UID), int(e.GID))

	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		for name, value := range xattrs {
			_ = unix.Lsetxattr(path, name, value, 0)
		}
	}

	return nil
}
