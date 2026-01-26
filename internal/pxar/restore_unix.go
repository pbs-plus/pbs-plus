//go:build unix

package pxar

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const (
	ACL_EA_VERSION         = 0x0002
	XATTR_NAME_ACL_ACCESS  = "system.posix_acl_access"
	XATTR_NAME_ACL_DEFAULT = "system.posix_acl_default"
)

func applyMeta(ctx context.Context, client *Client, file *os.File, e EntryInfo, fsCap filesystemCapabilities) error {
	defer file.Close()

	fd := int(file.Fd())

	_ = unix.Fchmod(fd, uint32(e.Mode&0777))

	uid, gid := int(e.UID), int(e.GID)

	atime := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	mtime := atime

	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)

	if err == nil && len(xattrs) > 0 {
		if d, ok := xattrs["user.owner"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				uid = id
			}
			delete(xattrs, "user.owner")
		}
		if d, ok := xattrs["user.group"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				gid = id
			}
			delete(xattrs, "user.group")
		}

		if d, ok := xattrs["user.lastaccesstime"]; ok {
			ts := binary.LittleEndian.Uint64(d)
			atime = time.Unix(int64(ts), 0)

			delete(xattrs, "user.lastaccesstime")
		}
		if d, ok := xattrs["user.lastwritetime"]; ok {
			ts := binary.LittleEndian.Uint64(d)
			mtime = time.Unix(int64(ts), 0)

			delete(xattrs, "user.lastwritetime")
		}

		if fsCap.supportsACLs {
			if d, ok := xattrs["user.acls"]; ok {
				var entries []types.PosixACL
				if cbor.Unmarshal(d, &entries) == nil {
					applyUnixACLsFd(fd, entries)
				}
				delete(xattrs, "user.acls")
			}
		} else {
			delete(xattrs, "user.acls")
		}

		if fsCap.supportsXAttrs {
			for name, val := range xattrs {
				switch name {
				case "user.creationtime", "user.fileattributes":
					continue
				default:
					_ = unix.Fsetxattr(fd, name, val, 0)
				}
			}
		}
	}

	if fsCap.supportsChown {
		_ = unix.Fchown(fd, uid, gid)
	}

	tv := []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	}
	_ = unix.Futimes(fd, tv)

	return nil
}

func applyUnixACLsFd(fd int, entries []types.PosixACL) {
	var acc, def []types.PosixACL
	for _, ent := range entries {
		if ent.IsDefault {
			def = append(def, ent)
		} else {
			acc = append(acc, ent)
		}
	}
	if len(acc) > 0 {
		_ = unix.Fsetxattr(fd, XATTR_NAME_ACL_ACCESS, packACL(acc), 0)
	}
	if len(def) > 0 {
		_ = unix.Fsetxattr(fd, XATTR_NAME_ACL_DEFAULT, packACL(def), 0)
	}
}

func packACL(entries []types.PosixACL) []byte {
	buf := make([]byte, 4+(8*len(entries)))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(ACL_EA_VERSION))
	tags := map[string]uint16{
		"user_obj": 0x01, "user": 0x02, "group_obj": 0x04,
		"group": 0x08, "mask": 0x10, "other": 0x20,
	}
	for i, ent := range entries {
		off := 4 + (i * 8)
		binary.LittleEndian.PutUint16(buf[off:off+2], tags[ent.Tag])
		binary.LittleEndian.PutUint16(buf[off+2:off+4], uint16(ent.Perms))
		binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(ent.ID))
	}
	return buf
}

func applyMetaSymlink(ctx context.Context, client *Client, path string, e EntryInfo, fsCap filesystemCapabilities) error {
	uid, gid := int(e.UID), int(e.GID)
	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)

	if err == nil && len(xattrs) > 0 {
		if d, ok := xattrs["user.owner"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				uid = id
			}
		}
		if d, ok := xattrs["user.group"]; ok {
			if id, err := strconv.Atoi(string(d)); err == nil {
				gid = id
			}
		}
	}

	if fsCap.supportsChown {
		_ = unix.Lchown(path, uid, gid)
	}

	if err == nil && fsCap.supportsXAttrs {
		for name, val := range xattrs {
			switch name {
			case "user.owner", "user.group", "user.acls", "user.fileattributes",
				"user.lastaccesstime", "user.lastwritetime", "user.creationtime":
				continue
			default:
				_ = unix.Lsetxattr(path, name, val, 0)
			}
		}
	}
	return nil
}

func restoreDir(ctx context.Context, client *Client, dst string, dirEntry EntryInfo, jobs chan<- restoreJob, fsCap filesystemCapabilities, wg *sync.WaitGroup, noAttr bool) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := client.ReadDir(ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())

		switch e.FileType {
		case FileTypeDirectory, FileTypeFile, FileTypeSymlink:
			wg.Add(1)
			go func(t string, info EntryInfo) {
				select {
				case jobs <- restoreJob{dest: t, info: info}:
				case <-ctx.Done():
					wg.Done()
				}
			}(target, e)

		case FileTypeFifo, FileTypeSocket:
			var opErr error
			mode := uint32(e.Mode & 0777)
			if e.FileType == FileTypeFifo {
				opErr = syscall.Mkfifo(target, mode)
			} else {
				opErr = syscall.Mknod(target, syscall.S_IFSOCK|mode, 0)
			}

			if opErr == nil || os.IsExist(opErr) {
				if f, openErr := os.OpenFile(target, os.O_RDONLY, 0); openErr == nil {
					if !noAttr {
						_ = applyMeta(ctx, client, f, e, fsCap)
					} else {
						f.Close()
					}
				}
			}
		}
	}

	if noAttr {
		return nil
	}

	df, err := os.Open(dst)
	if err != nil {
		return err
	}
	return applyMeta(ctx, client, df, dirEntry, fsCap)
}
