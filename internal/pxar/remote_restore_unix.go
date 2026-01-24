//go:build unix

package pxar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"golang.org/x/sys/unix"
)

const (
	ACL_EA_VERSION         = 0x0002
	XATTR_NAME_ACL_ACCESS  = "system.posix_acl_access"
	XATTR_NAME_ACL_DEFAULT = "system.posix_acl_default"
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, file *os.File, e EntryInfo) error {
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
			if ts, err := binary.ReadVarint(bytes.NewReader(d)); err == nil {
				atime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastaccesstime")
		}
		if d, ok := xattrs["user.lastwritetime"]; ok {
			if ts, err := binary.ReadVarint(bytes.NewReader(d)); err == nil {
				mtime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastwritetime")
		}

		if d, ok := xattrs["user.acls"]; ok {
			var entries []types.PosixACL
			if json.Unmarshal(d, &entries) == nil {
				applyUnixACLsFd(fd, entries)
			}
			delete(xattrs, "user.acls")
		}

		for name, val := range xattrs {
			switch name {
			case "user.creationtime", "user.fileattributes":
				continue
			default:
				_ = unix.Fsetxattr(fd, name, val, 0)
			}
		}
	}

	_ = unix.Fchown(fd, uid, gid)

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

func remoteApplyMetaSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
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

	_ = unix.Lchown(path, uid, gid)

	if err == nil {
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
