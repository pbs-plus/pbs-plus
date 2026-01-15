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
	FS_IMMUTABLE_FL        = 0x10
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	_ = os.Chmod(path, os.FileMode(e.Mode&0777))

	uid, gid := int(e.UID), int(e.GID)

	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
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

		if data, ok := xattrs["user.fileattributes"]; ok {
			var attrs map[string]bool
			if err := json.Unmarshal(data, &attrs); err == nil {
				if attrs["immutable"] {
					_ = applyImmutable(path, true)
				}
			}
			delete(xattrs, "user.fileattributes")
		}

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

func applyUnixACLs(path string, entries []types.PosixACL) error {
	var accessEntries []types.PosixACL
	var defaultEntries []types.PosixACL

	for _, e := range entries {
		if e.IsDefault {
			defaultEntries = append(defaultEntries, e)
		} else {
			accessEntries = append(accessEntries, e)
		}
	}

	if len(accessEntries) > 0 {
		_ = unix.Setxattr(path, XATTR_NAME_ACL_ACCESS, packACL(accessEntries), 0)
	}
	if len(defaultEntries) > 0 {
		_ = unix.Setxattr(path, XATTR_NAME_ACL_DEFAULT, packACL(defaultEntries), 0)
	}
	return nil
}

func packACL(entries []types.PosixACL) []byte {
	buf := make([]byte, 4+(8*len(entries)))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(ACL_EA_VERSION))
	for i, e := range entries {
		off := 4 + (i * 8)
		var tag uint16
		switch e.Tag {
		case "user_obj":
			tag = 0x01
		case "user":
			tag = 0x02
		case "group_obj":
			tag = 0x04
		case "group":
			tag = 0x08
		case "mask":
			tag = 0x10
		case "other":
			tag = 0x20
		}
		binary.LittleEndian.PutUint16(buf[off:off+2], tag)
		binary.LittleEndian.PutUint16(buf[off+2:off+4], uint16(e.Perms))
		binary.LittleEndian.PutUint32(buf[off+4:off+8], uint32(e.ID))
	}
	return buf
}

func remoteApplyMetaSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	uid, gid := int(e.UID), int(e.GID)

	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
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
			// Skip virtual metadata for symlinks as well
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
