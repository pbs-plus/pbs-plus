//go:build unix

package pxar

import (
	"context"
	"encoding/binary"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/format"
	"golang.org/x/sys/unix"
)

const (
	ACL_EA_VERSION         = 0x0002
	XATTR_NAME_ACL_ACCESS  = "system.posix_acl_access"
	XATTR_NAME_ACL_DEFAULT = "system.posix_acl_default"
)

func applyMeta(ctx context.Context, st *restoreState, file *os.File, e pxar.FileInfo) error {
	defer file.Close()

	fd := int(file.Fd())

	uid, gid := int(e.RawUID), int(e.RawGID)

	atime := format.StatxTimestamp{Secs: e.MtimeSecs, Nanos: e.MtimeNsecs}.Time()
	mtime := atime

	xattrs, err := st.client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)

	// chown BEFORE chmod: on Linux chown(2) clears the set-user-ID and
	// set-group-ID bits, so applying the mode first and then changing the
	// owner would silently strip suid/sgid from restored binaries. Doing
	// chown first lets the subsequent Fchmod reinstate them.
	if st.fsCap.supportsChown {
		_ = unix.Fchown(fd, uid, gid)
	}

	_ = unix.Fchmod(fd, uint32(e.RawMode&0777))

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
			if ts, ok := parseXattrUnixSecs(d); ok {
				atime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastaccesstime")
		}
		if d, ok := xattrs["user.lastwritetime"]; ok {
			if ts, ok := parseXattrUnixSecs(d); ok {
				mtime = time.Unix(ts, 0)
			}
			delete(xattrs, "user.lastwritetime")
		}

		if st.fsCap.supportsACLs {
			if d, ok := xattrs["user.acls"]; ok {
				// Only POSIX ACLs can be represented on a Unix destination. A
				// Windows-source payload ([]WinACL) would otherwise be decoded
				// into zero-value PosixACL entries and written as a corrupt
				// system.posix_acl_access/default xattr.
				if detectACLFlavor(d) == aclPosix {
					var entries []types.PosixACL
					if cbor.Unmarshal(d, &entries) == nil {
						applyUnixACLsFd(fd, entries)
					}
				}
				delete(xattrs, "user.acls")
			}
		} else {
			delete(xattrs, "user.acls")
		}

		// Re-apply ownership if the xattrs overrode uid/gid (user.owner/group).
		// Done after the parse above so the restored numeric ids win; chmod
		// has already run, so suid/sgid are preserved.
		if st.fsCap.supportsChown && (uid != int(e.RawUID) || gid != int(e.RawGID)) {
			_ = unix.Fchown(fd, uid, gid)
			// chown may have cleared suid/sgid again; reinstate the mode.
			_ = unix.Fchmod(fd, uint32(e.RawMode&0777))
		}

		if st.fsCap.supportsXAttrs {
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

	tv := []unix.Timeval{
		unix.NsecToTimeval(atime.UnixNano()),
		unix.NsecToTimeval(mtime.UnixNano()),
	}
	_ = unix.Futimes(fd, tv)

	return nil
}

func applyUnixACLsFd(fd int, entries []types.PosixACL) {
	knownTags := map[string]struct{}{
		"user_obj": {}, "user": {}, "group_obj": {},
		"group": {}, "mask": {}, "other": {},
	}
	var acc, def []types.PosixACL
	for _, ent := range entries {
		// Skip entries that don't carry a recognized POSIX ACL tag; an
		// empty/unknown tag would otherwise be packed as tag 0 and corrupt
		// the on-disk ACL.
		if _, ok := knownTags[ent.Tag]; !ok {
			continue
		}
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

func applyMetaSymlink(ctx context.Context, st *restoreState, path string, e pxar.FileInfo) error {
	uid, gid := int(e.RawUID), int(e.RawGID)
	xattrs, err := st.client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)

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

	if st.fsCap.supportsChown {
		_ = unix.Lchown(path, uid, gid)
	}

	if err == nil && st.fsCap.supportsXAttrs {
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

// applyTempMode gives a freshly-written temp file sensible permissions before
// its atomic rename in no-attr mode (os.CreateTemp creates files 0600). When
// the archive recorded a mode it is honored; otherwise 0666 matches the
// previous direct OpenFile(..., 0666) behavior (subject to the process umask).
func applyTempMode(path string, rawMode uint64) error {
	mode := os.FileMode(0o666)
	if rawMode != 0 {
		mode = os.FileMode(rawMode & 0o777)
	}
	return os.Chmod(path, mode)
}

func restoreDir(ctx context.Context, st *restoreState, job restoreJob) error {
	dst := job.dest
	dirEntry := job.info
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := st.client.ReadDir(ctx, dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())
		childSrc := path.Join(job.srcPath, e.Name())

		switch e.FileType {
		case pxar.FileTypeDirectory, pxar.FileTypeFile, pxar.FileTypeSymlink, pxar.FileTypeHardlink:
			st.wg.Add(1)
			go func(t, s string, info pxar.FileInfo) {
				select {
				case st.jobs <- restoreJob{dest: t, srcPath: s, info: info}:
				case <-ctx.Done():
					st.wg.Done()
				}
			}(target, childSrc, e)

		case pxar.FileTypeFifo, pxar.FileTypeSocket:
			var opErr error
			mode := uint32(e.RawMode & 0777)
			if e.FileType == pxar.FileTypeFifo {
				opErr = syscall.Mkfifo(target, mode)
			} else {
				opErr = syscall.Mknod(target, syscall.S_IFSOCK|mode, 0)
			}

			if opErr == nil || os.IsExist(opErr) {
				if f, openErr := os.OpenFile(target, os.O_RDONLY, 0); openErr == nil {
					if !st.noAttr {
						_ = applyMeta(ctx, st, f, e)
					} else {
						f.Close()
					}
				}
			}
		}
	}

	if st.noAttr {
		return nil
	}

	df, err := os.Open(dst)
	if err != nil {
		return err
	}
	return applyMeta(ctx, st, df, dirEntry)
}
