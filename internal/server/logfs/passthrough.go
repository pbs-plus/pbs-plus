//go:build linux

package logfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// taskLogPrefix is the relative path prefix for individual task log files.
// Task logs live at "tasks/XX/<UPID>".
const taskLogPrefix = "tasks/"

// LogFS holds the state of the active FUSE log filesystem. Set by Mount.
var LogFS *MountInfo

// MountInfo holds the backing and mount paths for the log FUSE.
type MountInfo struct {
	BackingDir string
	MountPoint string
}

// BackingTasksPath returns the path to the tasks directory in the
// backing filesystem, bypassing FUSE. Use for batch operations like
// junk log cleanup that should not generate FUSE events.
func (m *MountInfo) BackingTasksPath() string {
	return filepath.Join(m.BackingDir, "tasks")
}

// PassthroughNode wraps LoopbackNode to intercept filesystem operations
// and emit events to the EventBus.
type PassthroughNode struct {
	fs.LoopbackNode
	root *Root
}

var _ = (fs.NodeCreater)((*PassthroughNode)(nil))
var _ = (fs.NodeRenamer)((*PassthroughNode)(nil))
var _ = (fs.NodeUnlinker)((*PassthroughNode)(nil))

// Create intercepts file creation.
func (n *PassthroughNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	inode, fh, fuseFlags, errno = n.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno == 0 {
		fullRelPath := filepath.Join(n.relativePath(), name)
		n.root.bus.Emit(Event{
			Kind: EventCreate,
			Path: fullRelPath,
		})
	}
	return
}

// Rename intercepts file renames.
func (n *PassthroughNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	errno := n.LoopbackNode.Rename(ctx, name, newParent, newName, flags)
	if errno == 0 {
		oldPath := filepath.Join(n.relativePath(), name)
		n.root.bus.Emit(Event{
			Kind: EventRename,
			Path: oldPath,
		})
	}
	return errno
}

// Unlink intercepts file deletion.
func (n *PassthroughNode) Unlink(ctx context.Context, name string) syscall.Errno {
	errno := n.LoopbackNode.Unlink(ctx, name)
	if errno == 0 {
		n.root.bus.Emit(Event{
			Kind: EventUnlink,
			Path: filepath.Join(n.relativePath(), name),
		})
	}
	return errno
}

// relativePath returns the path relative to the FUSE mount root.
func (n *PassthroughNode) relativePath() string {
	return n.Path(n.root.EmbeddedInode())
}

// Open returns a file handle. For task log files, active, and archive,
// it wraps the handle to intercept writes and serialize concurrent
// access to the same file.
func (n *PassthroughNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fh, fuseFlags, errno = n.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return
	}

	relPath := n.relativePath()
	if isTaskLog(relPath) || isTaskIndex(relPath) {
		mu := n.root.lockFor(relPath)
		fh = &writeInterceptor{
			FileHandle: fh,
			root:       n.root,
			relPath:    relPath,
			mu:         mu,
		}
	}
	return
}

// writeInterceptor wraps a FileHandle. It serializes writes to the same
// file through a per-path mutex and emits events to the EventBus.
//
// PBS's worker task framework writes to task log files sequentially from
// a single fd (no file locking). pbs-plus also appends client logs to
// the same file through the FUSE mount. The mutex ensures these
// concurrent writers don't interleave partial writes.
type writeInterceptor struct {
	fs.FileHandle
	root    *Root
	relPath string
	mu      *sync.Mutex
}

var _ = (fs.FileWriter)((*writeInterceptor)(nil))

func (w *writeInterceptor) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	w.mu.Lock()
	defer w.mu.Unlock()

	written, errno := w.FileHandle.(fs.FileWriter).Write(ctx, data, off)
	if errno == 0 && written > 0 {
		w.root.bus.Emit(Event{
			Kind: EventWrite,
			Path: w.relPath,
			Data: data[:written],
		})
	}
	return written, errno
}

// isTaskLog returns true if the relative path looks like an individual
// PBS task log file: "tasks/XX/<UPID>".
func isTaskLog(relPath string) bool {
	if !strings.HasPrefix(relPath, taskLogPrefix) {
		return false
	}
	rest := relPath[len(taskLogPrefix):]
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 2 {
		return false
	}
	return len(parts[0]) == 2 && strings.HasPrefix(parts[1], "UPID:")
}

// isTaskIndex returns true if the relative path is the active or
// archive task index file.
func isTaskIndex(relPath string) bool {
	return relPath == "tasks/active" || strings.HasPrefix(relPath, "tasks/archive")
}

// Root is the FUSE root inode. It carries the EventBus and per-file
// write locks shared by all child nodes.
type Root struct {
	fs.LoopbackNode
	bus        *EventBus
	locks      sync.Map // string -> *sync.Mutex
	BackingDir string
}

// lockFor returns (or creates) a mutex for serializing writes to the
// given relative path. All file handles opened for the same path share
// the same mutex.
func (r *Root) lockFor(relPath string) *sync.Mutex {
	val, _ := r.locks.LoadOrStore(relPath, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// MountOptions returns FUSE mount options suitable for a log passthrough.
func MountOptions(allowOther bool) *fs.Options {
	return &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:     "pbs-logfs",
			Name:       "pbs-logfs",
			AllowOther: allowOther,
			Options:    []string{"default_permissions", "allow_other"},
		},
	}
}

// Mount mounts the FUSE passthrough filesystem. backingDir is the real
// directory that holds the PBS logs. mountPoint is where to mount.
func Mount(backingDir, mountPoint string, bus *EventBus, opts *fs.Options) (*fuse.Server, error) {
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return nil, err
	}

	root := &Root{bus: bus, BackingDir: backingDir}
	root.RootData = &fs.LoopbackRoot{
		Path:     backingDir,
		NewNode:  newNode,
		RootNode: root,
	}

	var st syscall.Stat_t
	if err := syscall.Stat(backingDir, &st); err != nil {
		return nil, err
	}
	root.RootData.Dev = uint64(st.Dev)

	if opts == nil {
		opts = MountOptions(true)
	}

	server, err := fs.Mount(mountPoint, root, opts)
	if err != nil {
		return nil, err
	}

	LogFS = &MountInfo{
		BackingDir: backingDir,
		MountPoint: mountPoint,
	}

	return server, nil
}

// newNode creates a PassthroughNode for all children.
func newNode(rootData *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	root := rootData.RootNode.(*Root)
	n := &PassthroughNode{
		root: root,
	}
	n.RootData = rootData
	return n
}
