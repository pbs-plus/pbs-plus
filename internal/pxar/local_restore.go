//go:build linux

package pxar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

func LocalRestore(
	ctx context.Context,
	pr *PxarReader,
	sourceDirs []string,
	destDir string,
	errChan chan error,
) {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		errChan <- fmt.Errorf("mkdir root: %w", err)
		return
	}

	for _, source := range sourceDirs {
		if err := ctx.Err(); err != nil {
			errChan <- err
			break
		}

		sourceAttr, err := pr.LookupByPath(source)
		if err != nil {
			errChan <- err
			continue
		}

		if sourceAttr.IsDir() {
			err = localRestoreDir(ctx, pr, destDir, sourceAttr)
			if err != nil {
				errChan <- err
				continue
			}
		} else {
			path := filepath.Join(destDir, sourceAttr.Name())
			err = localRestoreFile(ctx, pr, path, sourceAttr)
			if err != nil {
				errChan <- err
				continue
			}
		}
	}
}

func localRestoreFile(
	ctx context.Context,
	pr *PxarReader,
	path string,
	e *EntryInfo,
) error {
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
			if err := ctx.Err(); err != nil {
				return err
			}

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

func localRestoreSymlink(
	ctx context.Context,
	pr *PxarReader,
	path string,
	e *EntryInfo,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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

	uid, gid := int(e.UID), int(e.GID)
	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)

	if err == nil && len(xattrs) > 0 {
		if data, ok := xattrs["user.owner"]; ok {
			if parsed, err := strconv.Atoi(string(data)); err == nil {
				uid = parsed
			}
			delete(xattrs, "user.owner")
		}
		if data, ok := xattrs["user.group"]; ok {
			if parsed, err := strconv.Atoi(string(data)); err == nil {
				gid = parsed
			}
			delete(xattrs, "user.group")
		}

		_ = os.Chown(path, uid, gid)

		delete(xattrs, "user.fileattributes")

		if data, ok := xattrs["user.acls"]; ok {
			var entries []types.PosixACL
			if err := json.Unmarshal(data, &entries); err == nil {
				_ = applyUnixACLs(path, entries)
			}
			delete(xattrs, "user.acls")
		}

		var atime, mtime time.Time
		hasAtime, hasMtime := false, false

		if data, ok := xattrs["user.lastaccesstime"]; ok {
			if ts, err := binary.ReadVarint(bytes.NewReader(data)); err == nil {
				atime = time.Unix(ts, 0)
				hasAtime = true
			}
			delete(xattrs, "user.lastaccesstime")
		}

		if data, ok := xattrs["user.lastwritetime"]; ok {
			if ts, err := binary.ReadVarint(bytes.NewReader(data)); err == nil {
				mtime = time.Unix(ts, 0)
				hasMtime = true
			}
			delete(xattrs, "user.lastwritetime")
		}

		if hasAtime || hasMtime {
			if !hasAtime {
				atime = time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
			}
			if !hasMtime {
				mtime = time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
			}
			_ = os.Chtimes(path, atime, mtime)
		} else {
			mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
			_ = os.Chtimes(path, mt, mt)
		}

		for name, value := range xattrs {
			if name == "user.creationtime" {
				continue
			}
			_ = unix.Setxattr(path, name, value, 0)
		}
	} else {
		_ = os.Chown(path, uid, gid)
		mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
		_ = os.Chtimes(path, mt, mt)
	}

	return nil
}

func localApplyMetaSymlink(pr *PxarReader, path string, e *EntryInfo) error {
	uid, gid := int(e.UID), int(e.GID)
	xattrs, err := pr.ListXAttrs(e.EntryRangeStart, e.EntryRangeEnd)

	if err == nil && len(xattrs) > 0 {
		if data, ok := xattrs["user.owner"]; ok {
			if parsed, err := strconv.Atoi(string(data)); err == nil {
				uid = parsed
			}
		}
		if data, ok := xattrs["user.group"]; ok {
			if parsed, err := strconv.Atoi(string(data)); err == nil {
				gid = parsed
			}
		}
	}

	_ = os.Lchown(path, uid, gid)

	if err == nil && len(xattrs) > 0 {
		for name, value := range xattrs {
			if name == "user.owner" || name == "user.group" || name == "user.acls" ||
				name == "user.fileattributes" || name == "user.lastaccesstime" ||
				name == "user.lastwritetime" || name == "user.creationtime" {
				continue
			}
			_ = unix.Lsetxattr(path, name, value, 0)
		}
	}

	return nil
}
