//go:build linux

package logfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// taskLogPrefix is the relative path prefix for individual task log files.
// Task logs live at "tasks/XX/<UPID>".
const taskLogPrefix = "tasks/"

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
// it wraps the handle to intercept writes.
func (n *PassthroughNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fh, fuseFlags, errno = n.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return
	}

	relPath := n.relativePath()
	if isTaskLog(relPath) || isTaskIndex(relPath) {
		fh = &writeInterceptor{
			FileHandle: fh,
			root:       n.root,
			relPath:    relPath,
		}
	}
	return
}

// writeInterceptor wraps a FileHandle and emits Write events for
// task log files, the active index, and the archive index.
type writeInterceptor struct {
	fs.FileHandle
	root    *Root
	relPath string
}

var _ = (fs.FileWriter)((*writeInterceptor)(nil))

func (w *writeInterceptor) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
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

// Root is the FUSE root inode. It carries the EventBus reference
// shared by all child nodes.
type Root struct {
	fs.LoopbackNode
	bus *EventBus
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

	root := &Root{bus: bus}
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

	return fs.Mount(mountPoint, root, opts)
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
