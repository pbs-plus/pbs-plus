package pxarmount

import (
	"errors"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	pxar "github.com/pbs-plus/pxar"
	"golang.org/x/sys/unix"
)

func (fs *MutableFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if status, handled := fs.resolveCheck(path, re); !handled {
		return 0, status
	}

	// Priority: passthrough → journal → default ACL → pxar.

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Getxattr(abs, attr, dest)
		if xerr == nil {
			return uint32(sz), fuse.OK
		}
	}

	// 2. Journal.
	if re.Node != nil {
		val, err := fs.journal.GetXAttr(re.Node.ID, attr)
		if err != nil {
			return 0, fuse.EIO
		}
		if val != nil {
			return xattrValue(val, dest)
		}
	}

	// 3. Default ACL (virtual, no write).
	switch attr {
	case "system.posix_acl_access":
		if fs.acl.HasACLs() {
			return xattrValue(MarshalACL(fs.acl.ACLEntries), dest)
		}
	case "system.posix_acl_default":
		if re.IsDir && len(fs.acl.DefaultACLEntries) > 0 {
			return xattrValue(MarshalACL(fs.acl.DefaultACLEntries), dest)
		}
		return 0, fuse.Status(syscall.ENODATA)
	}

	// 4. Pxar archive.
	if re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		return fs.pxar.GetXAttr(cancel, &pxarHeader, attr, dest)
	}

	return 0, fuse.Status(syscall.ENODATA)
}

func (fs *MutableFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return 0, fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if status, handled := fs.resolveCheck(path, re); !handled {
		return 0, status
	}

	nameSet := make(map[string]bool)

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		sz, xerr := unix.Listxattr(abs, nil)
		if xerr == nil && sz > 0 {
			buf := make([]byte, sz)
			if sz, xerr = unix.Listxattr(abs, buf); xerr == nil {
				start := 0
				for i := 0; i <= sz; i++ {
					if i == sz || buf[i] == 0 {
						if i > start {
							nameSet[string(buf[start:i])] = true
						}
						start = i + 1
					}
				}
			}
		}
	}

	// 2. Journal xattrs.
	if re.Node != nil {
		names, err := fs.journal.ListXAttrs(re.Node.ID)
		if err != nil {
			log.Error(err, "")
		}
		for _, n := range names {
			nameSet[n] = true
		}
	}

	// 3. Default ACL xattr names (virtual).
	if fs.acl.HasACLs() {
		nameSet["system.posix_acl_access"] = true
		if re.IsDir && len(fs.acl.DefaultACLEntries) > 0 {
			nameSet["system.posix_acl_default"] = true
		}
	}

	// 4. Pxar xattrs.
	if re.PxarNode != nil {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		pxarSz, pxarStatus := fs.pxar.ListXAttr(cancel, &pxarHeader, nil)
		if pxarStatus == fuse.OK && pxarSz > 0 {
			buf := make([]byte, pxarSz)
			sz, status := fs.pxar.ListXAttr(cancel, &pxarHeader, buf)
			if status == fuse.OK {
				start := 0
				for i := 0; i <= int(sz); i++ {
					if i == int(sz) || buf[i] == 0 {
						if i > start {
							nameSet[string(buf[start:i])] = true
						}
						start = i + 1
					}
				}
			}
		}
	}

	var total uint32
	for n := range nameSet {
		total += uint32(len(n)) + 1
	}
	if dest == nil {
		return total, fuse.OK
	}
	if uint32(len(dest)) < total {
		return 0, fuse.Status(syscall.ERANGE)
	}
	pos := 0
	for n := range nameSet {
		pos += copy(dest[pos:], n)
		dest[pos] = 0
		pos++
	}
	return uint32(pos), fuse.OK
}

func (fs *MutableFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	fs.waitIfFrozen()
	path := fs.inodeToPath(input.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return status
	}
	fs.ensureNode(re)

	if re.Node == nil {
		return fuse.EIO
	}

	if err := fs.journal.SetXAttr(re.Node.ID, attr, data); err != nil {
		return fuse.EIO
	}

	if re.DataIsMut {
		abs := fs.mutablePath(path)
		flags := 0
		if input.Flags&XattrCreate != 0 {
			flags = unix.XATTR_CREATE
		} else if input.Flags&XattrReplace != 0 {
			flags = unix.XATTR_REPLACE
		}
		if err := unix.Setxattr(abs, attr, data, flags); err != nil {
			fs.logNonFatal("setxattr", attr, err)
		}
	}

	return fuse.OK
}

func (fs *MutableFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	fs.waitIfFrozen()
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return fuse.ENOENT
	}

	re, _ := fs.resolve(path)
	if re == nil || re.Node == nil {
		return fuse.OK
	}

	if err := fs.journal.RemoveXAttr(re.Node.ID, attr); err != nil {
		return fuse.EIO
	}

	if re.DataIsMut {
		if err := unix.Removexattr(fs.mutablePath(path), attr); err != nil {
			fs.logNonFatal("removexattr", attr, err)
		}
	}

	return fuse.OK
}

// applyPxarXattrsToFile sets extended attributes and file capabilities
// from a pxar entry onto a real file. Errors are logged but not fatal  -
func applyPxarXattrsToFile(abs string, entry *pxar.Entry) {
	for _, xa := range entry.Metadata.XAttrs {
		name := xa.Name()
		if len(name) == 0 {
			continue
		}
		// Skip ACL and fcaps  -  those are handled separately via the
		if isACLXattr(name) || isFcapsXattr(name) {
			continue
		}
		if err := unix.Lsetxattr(abs, string(name), xa.Value(), 0); err != nil {
			if !isIgnorableXattrErr(err) {
				log.Error(err, "non-fatal: copyUp xattr", "name", string(name), "path", abs)
			}
		}
	}
	if len(entry.Metadata.FCaps) > 0 {
		if err := unix.Lsetxattr(abs, "security.capability", entry.Metadata.FCaps, 0); err != nil {
			if !isIgnorableXattrErr(err) {
				log.Error(err, "non-fatal: copyUp fcaps", "path", abs)
			}
		}
	}
}

func isACLXattr(name []byte) bool {
	return bytesEq(name, "system.posix_acl_access") || bytesEq(name, "system.posix_acl_default")
}

func isFcapsXattr(name []byte) bool {
	return bytesEq(name, "security.capability")
}

// isIgnorableXattrErr reports whether an xattr error can be safely ignored

// isIgnorableXattrErr reports whether an xattr error can be safely ignored
func isIgnorableXattrErr(err error) bool {
	return errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENODATA) || errors.Is(err, unix.EOPNOTSUPP)
}

// Used as a fallback when os.Rename fails during Rename operations
// (e.g. cross-device rename).
