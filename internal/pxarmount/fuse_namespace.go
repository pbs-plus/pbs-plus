package pxarmount

import (
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (fs *MutableFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mkdir(abs, input.Mode&0o777); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeDir,
		Mode:    input.Mode&0o777 | syscall.S_IFDIR,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: false,
		Opaque:  hasPxar, // hide pxar children if shadowing
	}
	if hasPxar {
		node.RedirectTo = childPath // retain pxar source for metadata
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, name, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, true)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(input.NodeId)
	childPath := joinPath(parentPath, name)

	abs := fs.mutablePath(childPath)
	if err := syscall.Mknod(abs, input.Mode, int(input.Rdev)); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    input.Mode,
		UID:     input.Uid,
		GID:     input.Gid,
		Size:    0,
		MtimeNs: now,
		CtimeNs: now,
		HasData: true,
	}
	if hasPxar {
		node.RedirectTo = childPath
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, name, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, linkName string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, linkName)

	abs := fs.mutablePath(childPath)
	if err := syscall.Symlink(target, abs); err != nil {
		return fuse.ToStatus(err)
	}

	fs.applyACLOwnership(abs)

	hasPxar := fs.hasPxarEntry(childPath)
	now := time.Now().UnixNano()
	node := &GraphNode{
		Kind:       NodeSymlink,
		Mode:       uint32(syscall.S_IFLNK | 0o777),
		UID:        header.Uid,
		GID:        header.Gid,
		Size:       0,
		MtimeNs:    now,
		CtimeNs:    now,
		HasData:    true,
		SymlinkTgt: target,
	}
	if hasPxar {
		node.RedirectTo = childPath
	}

	parentID := fs.resolveParentNodeID(parentPath)

	// Atomically create node + edge + whiteout.
	nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(parentID, linkName, node, hasPxar)
	if err != nil {
		if err := os.Remove(abs); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return fuse.EIO
	}
	node.ID = nodeID

	ino := fs.pathToIno(childPath, false)

	out.NodeId = ino
	out.Generation = 1
	out.EntryValid = 1
	out.AttrValid = 1
	fillAttrFromNode(&out.Attr, node)
	out.Ino = ino
	return fuse.OK
}

func (fs *MutableFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}

	// Journal-first for destructive ops.
	parentID := fs.resolveParentNodeID(parentPath)

	if re.Node != nil {
		// Atomically remove edge + node + add whiteout if pxar counterpart exists.
		needsWhiteout := re.PxarNode != nil || fs.hasPxarEntry(childPath)
		if err := fs.journal.DeleteEdgeAndNode(parentID, name, re.Node.ID, needsWhiteout); err != nil {
			return fuse.EIO
		}
	} else if re.PxarNode != nil {
		// Pure pxar deletion: just add whiteout.
		if err := fs.journal.AddWhiteout(parentID, name); err != nil {
			return fuse.EIO
		}
	}

	if re.DataIsMut {
		if err := os.Remove(fs.mutablePath(childPath)); err != nil {
			fs.logNonFatal("remove", childPath, err)
		}
	}

	fs.unmapInode(childPath)
	return fuse.OK
}

func (fs *MutableFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	fs.waitIfFrozen()
	parentPath := fs.inodeToPath(header.NodeId)
	childPath := joinPath(parentPath, name)

	re, status := fs.resolve(childPath)
	if status != fuse.OK {
		return status
	}
	if !re.IsDir {
		return fuse.ENOTDIR
	}

	parentNodeID := fs.resolveParentNodeID(childPath)
	if parentNodeID != 0 {
		edges, err := fs.journal.ListEdges(parentNodeID)
		if err != nil {
			log.Error(err, "")
		}
		whiteouts, err := fs.journal.ListWhiteouts(parentNodeID)
		if err != nil {
			log.Error(err, "")
		}
		if len(edges) > 0 || len(whiteouts) > 0 {
			return fuse.Status(syscall.ENOTEMPTY)
		}
	}

	// Also check pxar children if not opaque.
	if re.Node == nil || !re.Node.Opaque {
		pxarDirPath := childPath
		if re.Node != nil && re.Node.RedirectTo != "" {
			pxarDirPath = re.Node.RedirectTo
		}
		if pxarNode := fs.findPxarNode(pxarDirPath); pxarNode != nil {
			entries, err := fs.pxar.ReadDirRaw(pxarNode.inode)
			if err != nil {
				log.Error(err, "")
			}
			if len(entries) > 0 {
				return fuse.Status(syscall.ENOTEMPTY)
			}
		}
	}

	return fs.Unlink(cancel, header, name)
}

func (fs *MutableFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	fs.waitIfFrozen()
	oldParentPath := fs.inodeToPath(input.NodeId)
	newParentPath := fs.inodeToPath(input.Newdir)
	oldPath := joinPath(oldParentPath, oldName)
	newPath := joinPath(newParentPath, newName)

	oldRE, oldStatus := fs.resolve(oldPath)
	if oldStatus != fuse.OK {
		return oldStatus
	}

	oldParentID := fs.resolveParentNodeID(oldParentPath)
	newParentID := fs.resolveParentNodeID(newParentPath)

	destHasPXar := fs.hasPxarEntry(newPath)
	destRE, _ := fs.resolve(newPath)
	var destNodeID int64
	if destRE != nil && destRE.Node != nil {
		destNodeID = destRE.Node.ID
	}

	// All journal mutations happen in a single SQLite transaction so a
	// crash at any point leaves the journal in a consistent state.
	if oldRE.Node != nil {
		// Source has a journal node: atomically move edge, replace dest, add whiteouts.
		whiteoutOld := oldRE.Node.RedirectTo != ""
		if err := fs.journal.MoveEdgeAndWhiteout(
			oldParentID, oldName, newParentID, newName,
			destNodeID, whiteoutOld, destHasPXar); err != nil {
			return fuse.EIO
		}
	} else {
		// Source is pxar-only: create journal node at destination, whiteout old.
		now := time.Now().UnixNano()
		node := &GraphNode{
			Kind:       nodeKindFromPxar(oldRE.PxarNode),
			Mode:       statMode(oldRE.PxarNode.mode),
			UID:        oldRE.PxarNode.uid,
			GID:        oldRE.PxarNode.gid,
			Size:       oldRE.PxarNode.fileSize,
			MtimeNs:    oldRE.PxarNode.mtimeSecs*1e9 + int64(oldRE.PxarNode.mtimeNanos),
			CtimeNs:    now,
			HasData:    false,
			RedirectTo: oldPath,
			SymlinkTgt: oldRE.SymlinkTgt,
		}
		// Use compound operation: delete dest edge+node if needed, create node+edge+whiteouts.
		if destNodeID != 0 {
			if err := fs.journal.DeleteEdgeAndNode(newParentID, newName, destNodeID, false); err != nil {
				return fuse.EIO
			}
		}
		nodeID, err := fs.journal.CreateNodeEdgeAndWhiteout(newParentID, newName, node, false)
		if err != nil {
			return fuse.EIO
		}
		node.ID = nodeID

		// Whiteout old location.
		if err := fs.journal.AddWhiteout(oldParentID, oldName); err != nil {
			return fuse.EIO
		}
		if destHasPXar {
			if err := fs.journal.AddWhiteout(newParentID, newName); err != nil {
				fs.logNonFatal("add-whiteout", newName, err)
			}
		}
		oldRE.Node = node
	}

	fs.unmapInode(newPath)
	ino := fs.pathToIno(oldPath, oldRE.IsDir)
	fs.unmapInode(oldPath)
	fs.mapInode(ino, newPath)

	if oldRE.IsDir {
		fs.remapPathPrefix(oldPath, newPath)
	}

	// If we crash here, the journal is consistent  -  disk files are redundant

	// Remove destination mutable data (journal already points away from it).
	if destRE != nil && destRE.DataIsMut {
		if err := os.Remove(fs.mutablePath(newPath)); err != nil {
			fs.logNonFatal("remove-dest", newPath, err)
		}
	}

	if oldRE.DataIsMut || oldRE.IsDir {
		oldAbs := fs.mutablePath(oldPath)
		if _, err := os.Stat(oldAbs); err == nil {
			newAbs := fs.mutablePath(newPath)
			if err := os.MkdirAll(filepath.Dir(newAbs), 0o755); err != nil {
				return fuse.ToStatus(err)
			}
			if err := os.Rename(oldAbs, newAbs); err != nil {
				// If copy also fails, the journal edge is still correct
				// and ReconcileMutableDir will clean up on next startup.
				fs.logNonFatal("rename-disk", oldPath, err)
				if !oldRE.IsDir {
					if copyErr := copyRegularFile(oldAbs, newAbs); copyErr != nil {
						fs.logNonFatal("copy-fallback", newPath, copyErr)
					}
				}
			}
		}
	}

	return fuse.OK
}

func (fs *MutableFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	path := fs.inodeToPath(header.NodeId)
	if path == "" {
		return nil, fuse.ENOENT
	}

	re, status := fs.resolve(path)
	if status != fuse.OK {
		return nil, status
	}

	if re.SymlinkTgt != "" {
		return []byte(re.SymlinkTgt), fuse.OK
	}
	if re.PxarNode != nil && re.PxarNode.isSymlink {
		pxarHeader := *header
		pxarHeader.NodeId = re.PxarNode.inode
		return fs.pxar.Readlink(cancel, &pxarHeader)
	}
	return nil, fuse.EINVAL
}

func (fs *MutableFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	fs.waitIfFrozen()
	return fuse.ENOSYS
}
