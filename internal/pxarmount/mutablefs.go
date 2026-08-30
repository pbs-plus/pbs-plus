package pxarmount

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/puzpuzpuz/xsync/v4"
)

// Applied to the journal in Flush (file close) or SetAttr
// (whichever runs first), serialized by per-inode lock.
type pendingMeta struct {
	size        uint64
	mtimeNs     int64
	ctimeNs     int64
	dataExtents []dataExtent
	writeErr    error
}

//   - PxarFS provides the immutable lower layer
//   - Journal provides the SQLite-backed inode graph for the overlay
//
// The journal uses a graph model (nodes + edges) making rename O(1):
//
//	Walk edges from root. If a component is whiteout → ENOENT.
//	If an edge is found → use the journal node (authoritative).
//	If no edge → fall back to pxar at the node's redirect_to path.

//   - PxarFS provides the immutable lower layer
//   - Journal provides the SQLite-backed inode graph for the overlay
//
// The journal uses a graph model (nodes + edges) making rename O(1):
//
//	Walk edges from root. If a component is whiteout → ENOENT.
//	If an edge is found → use the journal node (authoritative).
//	If no edge → fall back to pxar at the node's redirect_to path.
type MutableFS struct {
	fuse.RawFileSystem

	pxar       *PxarFS
	journal    *Journal
	mutableDir string

	// Inode allocation for pxar-only entries.
	nextIno atomic.Uint64

	// File handle management  -  lock-free via xsync.Map since every
	// Read/Write/Flush/Fsync calls getFh. nextFh uses atomic for
	handles *xsync.Map[uint64, *passFh]
	nextFh  atomic.Uint64

	// Per-inode writer locks.
	inoLocks *xsync.Map[uint64, *sync.RWMutex]

	mmapData [][]byte

	origSnapshot  snapshotRef
	pbsStore      string
	origPpxarDidx string

	acl     ACLConfig
	verbose bool

	// Tracks inodes with deferred Write metadata not yet flushed to
	// the journal. Keyed by FUSE inode (input.NodeId).
	dirtyMeta *xsync.Map[uint64, pendingMeta]

	mutationMu sync.RWMutex

	// Inode ↔ path bidirectional mapping.
	// Per-instance to prevent cross-mount corruption  -  analogous to
	// ext4's per-superblock inode cache. Uses xsync.Map for lock-free
	inoLookup  *xsync.Map[string, uint64]
	pathLookup *xsync.Map[uint64, string]

	// Per-path ensureNode serialization  -  prevents duplicate journal
	// nodes when concurrent FUSE ops (e.g. setfacl -R) materialize
	// the same pxar entry simultaneously.
	ensureLocks *xsync.Map[string, *sync.Mutex]
}

// NewMutableFS creates a layered filesystem with an immutable pxar base and a mutable overlay.

// NewMutableFS creates a layered filesystem with an immutable pxar base and a mutable overlay.
func NewMutableFS(pxar *PxarFS, journal *Journal, mutableDir string) *MutableFS {
	fs := &MutableFS{
		pxar:        pxar,
		journal:     journal,
		mutableDir:  mutableDir,
		handles:     xsync.NewMap[uint64, *passFh](),
		inoLocks:    xsync.NewMap[uint64, *sync.RWMutex](),
		inoLookup:   xsync.NewMap[string, uint64](),
		pathLookup:  xsync.NewMap[uint64, string](),
		ensureLocks: xsync.NewMap[string, *sync.Mutex](),
		dirtyMeta:   xsync.NewMap[uint64, pendingMeta](),
		nextIno:     atomic.Uint64{},
	}
	fs.nextIno.Store(1)
	return fs
}

func (fs *MutableFS) SetSnapshotRef(ref snapshotRef) { fs.origSnapshot = ref }

func (fs *MutableFS) SetACLConfig(cfg ACLConfig) { fs.acl = cfg }

// applyACL overrides the UID/GID and mode on a ResolvedEntry when the ACL
// config specifies a default owner, group, or mask.

// applyACL overrides the UID/GID and mode on a ResolvedEntry when the ACL
// config specifies a default owner, group, or mask.
func (fs *MutableFS) applyACL(re *ResolvedEntry) {
	if fs.acl.OwnerUID != 0 {
		re.UID = uint32(fs.acl.OwnerUID)
	}
	if fs.acl.OwnerGID != 0 {
		re.GID = uint32(fs.acl.OwnerGID)
	}

	// When ACL entries are present, the mode's group bits represent the ACL
	if fs.acl.HasACLs() {
		for _, e := range fs.acl.ACLEntries {
			if e.Tag == ACLMask {
				re.Mode = (re.Mode &^ 0070) | (uint32(e.Perm) << 3)
				break
			}
		}
	}
}

func (fs *MutableFS) SetVerbose(v bool) { fs.verbose = v }

func (fs *MutableFS) debugf(format string, args ...any) {
	if fs.verbose {
		fmt.Fprintf(os.Stderr, "  "+format+"\n", args...)
	}
}

func (fs *MutableFS) logNonFatal(op, path string, err error) {
	if fs.verbose {
		log.Error(err, "non-fatal error", "op", op, "path", path)
	}
}

func (fs *MutableFS) SetStorePaths(pbsStore, ppxarDidx string) {
	fs.pbsStore = pbsStore
	fs.origPpxarDidx = ppxarDidx
}

func (fs *MutableFS) InitMutableRoot() error {
	return os.MkdirAll(fs.mutableDir, 0o755)
}

// ReconcileMutableDir removes orphan disk entries not tracked by journal nodes.
// Called on startup to clean up after unclean shutdowns  -  analogous to
// ext4's orphan inode cleanup during journal recovery (ext4_orphan_cleanup).
// A file is an orphan if:
//   - No journal node exists for its path, OR
//   - The journal node exists but HasData is false
//
// Directories are kept (they may be parents of tracked files and are cheap).

// ReconcileMutableDir removes orphan disk entries not tracked by journal nodes.
// Called on startup to clean up after unclean shutdowns  -  analogous to
// ext4's orphan inode cleanup during journal recovery (ext4_orphan_cleanup).
// A file is an orphan if:
//   - No journal node exists for its path, OR
//   - The journal node exists but HasData is false
//
// Directories are kept (they may be parents of tracked files and are cheap).
func (fs *MutableFS) ReconcileMutableDir() error {
	updated := false
	err := filepath.Walk(fs.mutableDir, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}

		relPath, rerr := filepath.Rel(fs.mutableDir, absPath)
		if rerr != nil {
			return nil
		}

		if relPath == "." || relPath == JournalDir || strings.HasPrefix(relPath, JournalDir+string(filepath.Separator)) {
			return nil
		}

		fusePath := "/" + filepath.ToSlash(relPath)

		nodeID, pxarPath, _, _, rerr := fs.journal.ResolvePath(fusePath)
		if rerr != nil {
			return nil
		}

		if info.IsDir() {
			if nodeID != 0 {
				return nil
			}
			if pxarPath == "" {
				if err := os.Remove(absPath); err != nil {
					fs.logNonFatal("reconcile-remove", fusePath, err)
				}
				return nil
			}
			if aerr := fs.adoptOrphan(fusePath, info); aerr != nil {
				fs.logNonFatal("reconcile-adopt", fusePath, aerr)
				return nil
			}
			updated = true
			return nil
		}

		if nodeID == 0 {
			if pxarPath == "" {
				if err := os.Remove(absPath); err != nil {
					fs.logNonFatal("reconcile-remove", fusePath, err)
				}
				return nil
			}
			if aerr := fs.adoptOrphan(fusePath, info); aerr != nil {
				fs.logNonFatal("reconcile-adopt", fusePath, aerr)
				return nil
			}
			updated = true
			return nil
		}

		node, nerr := fs.journal.GetNode(nodeID)
		if nerr != nil || node == nil {
			return nil
		}

		if !node.HasData {
			if err := os.Remove(absPath); err != nil {
				fs.logNonFatal("reconcile-remove", fusePath, err)
			}
			return nil
		}

		stat := info.Sys().(*syscall.Stat_t)
		sizeStale := uint64(stat.Size) != node.Size

		extentsStale := false
		if node.SparseData {
			extents, eerr := rebuildDataExtents(absPath, uint64(info.Size()))
			if eerr != nil {
				fs.logNonFatal("reconcile-extents", fusePath, eerr)
			} else if merged := mergeDataExtents(node.DataExtents, extents); !slices.Equal(merged, node.DataExtents) {
				node.DataExtents = merged
				extentsStale = true
			}
		}

		if !sizeStale && !extentsStale {
			return nil
		}
		if sizeStale {
			node.Size = uint64(info.Size())
			node.MtimeNs = info.ModTime().UnixNano()
			node.CtimeNs = info.ModTime().UnixNano()
			if node.SparseData {
				node.LowerSize = min(node.LowerSize, node.Size)
				node.DataExtents = trimDataExtents(node.DataExtents, node.Size)
			}
		}
		if err := fs.journal.UpdateNode(node); err != nil {
			log.Error(err, "")
		}
		updated = true

		return nil
	})
	if err != nil {
		return err
	}
	if updated {
		return fs.journal.Sync()
	}
	return nil
}

// adoptOrphan re-registers a backing file whose journal node was lost to an
// unclean shutdown. Its whole extent range is backing data by construction.
func (fs *MutableFS) adoptOrphan(fusePath string, info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("adopt %q: no stat", fusePath)
	}
	mtime := info.ModTime().UnixNano()
	node := &GraphNode{
		Kind:    NodeFile,
		Mode:    stat.Mode,
		UID:     stat.Uid,
		GID:     stat.Gid,
		Size:    uint64(info.Size()),
		MtimeNs: mtime,
		CtimeNs: mtime,
		HasData: true,
	}
	if info.IsDir() {
		node.Kind = NodeDir
		node.Size = 0
		node.HasData = false
	}
	if lower := fs.findPxarNode(fusePath); lower != nil {
		node.RedirectTo = fusePath
		if node.Kind == NodeFile {
			node.SparseData = true
			node.LowerSize = lower.fileSize
			extents, err := rebuildDataExtents(fs.mutablePath(fusePath), node.Size)
			if err != nil {
				return err
			}
			node.DataExtents = extents
		}
	}
	_, err := fs.journal.EnsureNodePath(fusePath, node, false)
	return err
}

func (fs *MutableFS) Init(server *fuse.Server) {
	fs.RawFileSystem = fuse.NewDefaultRawFileSystem()
	fs.RawFileSystem.Init(server)
}

func (fs *MutableFS) String() string { return "pxar-mutable" }

func (fs *MutableFS) SetDebug(dbg bool) {}

func (fs *MutableFS) beginMutation() { fs.mutationMu.RLock() }

func (fs *MutableFS) endMutation() { fs.mutationMu.RUnlock() }

// resetAfterCommit clears in-memory state that became stale after a
// successful commit (journal cleared, pxar reader swapped).
func (fs *MutableFS) resetAfterCommit() {
	fs.dirtyMeta = xsync.NewMap[uint64, pendingMeta]()
}

func (fs *MutableFS) Close() {
	for _, d := range fs.mmapData {
		if err := munmap(d); err != nil {
			fs.logNonFatal("munmap", "data", err)
		}
	}
	fs.mmapData = nil

	fs.handles.Range(func(_ uint64, fh *passFh) bool {
		if err := syscall.Close(fh.fd); err != nil {
			fs.logNonFatal("close-fd", "fd", err)
		}
		return true
	})
	fs.handles = nil
}
