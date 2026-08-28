package pxarmount

import (
	"sync"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// resolve looks up a path using the inode graph, falling back to pxar.
func (fs *MutableFS) resolveRoot() (*ResolvedEntry, fuse.Status) {
	n, err := fs.journal.GetNode(1)
	fs.debugf("resolveRoot: GetNode(1) err=%v node=%+v", err, n)
	if err != nil {
		return nil, fuse.EIO
	}
	if n != nil {
		return fs.resolveFromNode("/", n)
	}
	// Fallback to pure pxar root
	pxarNode := fs.findPxarNode("/")
	if pxarNode == nil {
		return nil, fuse.ENOENT
	}
	re := &ResolvedEntry{
		Path:      "/",
		PxarNode:  pxarNode,
		DataIsMut: false,
		IsDir:     true,
		Mode:      statMode(pxarNode.mode),
		UID:       pxarNode.uid,
		GID:       pxarNode.gid,
		Size:      pxarNode.fileSize,
		MtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
	}
	re.Inode = fs.pathToIno("/", true)
	fs.applyACL(re)
	return re, fuse.OK
}

func (fs *MutableFS) resolve(path string) (*ResolvedEntry, fuse.Status) {
	// Root is always a pxar-only directory.
	if path == "/" || path == "" {
		return fs.resolveRoot()
	}
	nodeID, pxarPath, _, _, err := fs.journal.ResolvePath(path)
	if err != nil {
		fs.debugf("resolve(%q) ResolvePath err: %v", path, err)
		return nil, fuse.EIO
	}

	// Whiteout detected.
	if nodeID == 0 && pxarPath == "" {
		return nil, fuse.ENOENT
	}

	if nodeID != 0 {
		node, err := fs.journal.GetNode(nodeID)
		if err != nil {
			return nil, fuse.EIO
		}
		if node == nil {
			return nil, fuse.ENOENT
		}
		return fs.resolveFromNode(path, node)
	}

	// Fell off graph  -  check pxar.
	pxarNode := fs.findPxarNode(pxarPath)
	if pxarNode == nil {
		return nil, fuse.ENOENT
	}

	re := &ResolvedEntry{
		Path:      path,
		PxarNode:  pxarNode,
		DataIsMut: false,
		IsDir:     pxarNode.isDir,
		Mode:      statMode(pxarNode.mode),
		UID:       pxarNode.uid,
		GID:       pxarNode.gid,
		Size:      pxarNode.fileSize,
		MtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
		CtimeNs:   pxarNode.mtimeSecs*1e9 + int64(pxarNode.mtimeNanos),
	}
	// Use cached xattr-derived times if already resolved; otherwise fall
	// back to Stat.Mtime. Full resolution is deferred to individual
	// GetAttr calls so that bulk readdir never triggers O(N) archive reads.
	if pxarNode.timesResolved {
		aNs, mNs := fs.pxar.ResolvedTimes(pxarNode)
		re.AtimeNs = aNs
		re.MtimeNs = mNs
		re.CtimeNs = mNs
	} else {
		re.AtimeNs = re.MtimeNs
	}
	re.Inode = fs.pathToIno(path, re.IsDir)
	fs.applyACL(re)
	return re, fuse.OK
}

// resolveCheck is a helper for xattr ops that returns (status, ok) where

// resolveCheck is a helper for xattr ops that returns (status, ok) where
func (fs *MutableFS) resolveCheck(_ string, re *ResolvedEntry) (fuse.Status, bool) {
	if re == nil {
		return fuse.ENOENT, false
	}
	return fuse.OK, true
}

func (fs *MutableFS) resolveFromNode(path string, n *GraphNode) (*ResolvedEntry, fuse.Status) {
	re := &ResolvedEntry{
		Path:       path,
		Node:       n,
		DataIsMut:  n.HasData,
		IsDir:      n.Kind == NodeDir,
		Mode:       n.Mode,
		UID:        n.UID,
		GID:        n.GID,
		Size:       n.Size,
		MtimeNs:    n.MtimeNs,
		CtimeNs:    n.CtimeNs,
		SymlinkTgt: n.SymlinkTgt,
	}

	// Ensure mode has file type bits.
	re.Mode = ensureModeType(re.Mode, n.Kind)

	// If the node has a redirect, check pxar for data.
	if n.RedirectTo != "" && (!n.HasData || n.SparseData) {
		pxarNode := fs.findPxarNode(n.RedirectTo)
		re.PxarNode = pxarNode
		// Use pxar metadata if node fields are zero.
		if pxarNode != nil {
			if re.Size == 0 {
				re.Size = pxarNode.fileSize
			}
			if re.UID == 0 && re.GID == 0 {
				re.UID = pxarNode.uid
				re.GID = pxarNode.gid
			}
		}
	}

	re.Inode = fs.pathToIno(path, re.IsDir)
	fs.applyACL(re)
	return re, fuse.OK
}

func (fs *MutableFS) findPxarNode(path string) *node {
	if path == "/" {
		return fs.pxar.GetNode(RootInode)
	}

	curIno := RootInode
	parts := splitPath(path)

	for i, name := range parts {
		if name == "" {
			continue
		}
		entries, err := fs.pxar.ReadDirRaw(curIno)
		if err != nil {
			return nil
		}
		found := false
		for _, e := range entries {
			if e.name == name {
				// root inode is cached and subdirectories appear empty.
				n := fs.pxar.RegisterSlimNode(&e, curIno)
				if i == len(parts)-1 {
					return n
				}
				curIno = e.inode
				found = true
				break
			}
		}
		if !found {
			return nil
		}
	}
	return nil
}

func (fs *MutableFS) hasPxarEntry(path string) bool {
	return fs.findPxarNode(path) != nil
}

// resolveParentNodeID ensures a journal node exists for the parent path
// and returns its node ID. Creates pxar-derived nodes as needed.

// resolveParentNodeID ensures a journal node exists for the parent path
// and returns its node ID. Creates pxar-derived nodes as needed.
func (fs *MutableFS) resolveParentNodeID(parentPath string) int64 {
	if parentPath == "" || parentPath == "/" {
		return 1
	}

	re, status := fs.resolve(parentPath)
	if status != fuse.OK {
		return 1 // fallback to root
	}

	fs.ensureNode(re)
	if re.Node != nil {
		return re.Node.ID
	}
	return 1
}

// ensureNode ensures a journal node+edge exists for a resolved entry.
// For pxar-only entries, it creates a node with redirect_to and an edge
// under the parent. For journal entries, it's a no-op.
// Per-path locking prevents duplicate nodes when concurrent FUSE ops
// (e.g. setfacl -R) materialize the same pxar entry simultaneously  -
// analogous to ext4's inode_lock preventing concurrent inode initialization.

// ensureNode ensures a journal node+edge exists for a resolved entry.
// For pxar-only entries, it creates a node with redirect_to and an edge
// under the parent. For journal entries, it's a no-op.
// Per-path locking prevents duplicate nodes when concurrent FUSE ops
// (e.g. setfacl -R) materialize the same pxar entry simultaneously  -
// analogous to ext4's inode_lock preventing concurrent inode initialization.
func (fs *MutableFS) ensureNode(re *ResolvedEntry) {
	if re.Node != nil {
		return
	}

	// Acquire per-path lock to serialize concurrent ensureNode for the
	// same path. Without this, two concurrent setfacl threads could both
	// resolve the same path, see Node==nil, and create duplicate nodes.
	val, _ := fs.ensureLocks.LoadOrStore(re.Path, &sync.Mutex{})
	pathMu := val
	pathMu.Lock()
	defer pathMu.Unlock()

	// Double-check after acquiring lock  -  another goroutine may have
	if re.Node != nil {
		return
	}
	// Re-resolve: the other goroutine's node is visible via the journal.
	re2, status := fs.resolve(re.Path)
	if status == fuse.OK && re2.Node != nil {
		re.Node = re2.Node
		return
	}

	now := time.Now().UnixNano()
	node := &GraphNode{}
	kind := NodeFile
	if re.IsDir {
		kind = NodeDir
	}
	if re.SymlinkTgt != "" {
		kind = NodeSymlink
	}
	node.Kind = kind
	node.Mode = re.Mode
	node.UID = re.UID
	node.GID = re.GID
	node.Size = re.Size
	node.MtimeNs = re.MtimeNs
	node.CtimeNs = now
	node.HasData = re.DataIsMut
	node.SymlinkTgt = re.SymlinkTgt
	if re.PxarNode != nil {
		node.RedirectTo = re.Path
	}

	nodeID, err := fs.journal.EnsureNodePath(re.Path, node, false)
	if err != nil {
		fs.debugf("ensureNode: EnsureNodePath(%q) failed: %v", re.Path, err)
		return
	}
	node.ID = nodeID
	re.Node = node
}
