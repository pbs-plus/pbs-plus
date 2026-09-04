package pxarmount

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pxar/transfer"
	"golang.org/x/sys/unix"
)

// Stack is a snapshot filesystem built for serving without mounting it.
type Stack struct {
	Raw  fuse.RawFileSystem
	MFS  *MutableFS
	Sock net.Listener

	journal *Journal
}

// BuildStack constructs the PxarFS (MutableFS-wrapped for writable mounts)
// described by cfg; the caller owns the result and must Close it.
func BuildStack(cfg MountConfig) (*Stack, error) {
	reader, _ := cfg.Reader.(*transfer.SplitReader)
	if reader == nil && !cfg.InitMode {
		return nil, fmt.Errorf("mount config requires a *transfer.SplitReader reader")
	}
	pxarFS, err := NewPxarFS(reader)
	if err != nil {
		return nil, fmt.Errorf("creating pxar FS: %w", err)
	}
	pxarFS.SetVerbose(cfg.Verbose)

	stack := &Stack{Raw: pxarFS}

	backingDir := cfg.BackingDir
	if backingDir == "" && cfg.InitMode {
		backingDir = cfg.MountPoint + ".backing"
	}
	if backingDir == "" {
		return stack, nil
	}

	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating backing dir: %w", err)
	}

	journalDir := filepath.Join(backingDir, JournalDir)
	journal, err := OpenJournal(journalDir)
	if err != nil {
		return nil, fmt.Errorf("opening journal: %w", err)
	}

	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "  mutation mode, journal in %s\n", journalDir)
	}

	mfs := NewMutableFS(pxarFS, journal, backingDir)

	origSnap := ParseOrigSnapshot(cfg.PBSStore, cfg.OrigPpxarDidx)
	if cfg.InitMode {
		origSnap = SnapshotRefForInit(cfg.Namespace)
	}

	mfs.SetSnapshotRef(origSnap)
	mfs.SetACLConfig(cfg.ACL)
	mfs.SetStorePaths(cfg.PBSStore, cfg.OrigPpxarDidx)
	mfs.SetVerbose(cfg.Verbose)

	if err := mfs.InitMutableRoot(); err != nil {
		if err := journal.Close(); err != nil {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("initializing mutable root: %w", err)
	}

	if err := mfs.ReconcileMutableDir(); err != nil && cfg.Verbose {
		fmt.Fprintf(os.Stderr, "  warning: reconcile error: %v\n", err)
	}

	mfs.applyACLOwnership(backingDir)

	mfs.mapInode(RootInode, "/")

	stack.Raw = mfs
	stack.MFS = mfs
	stack.journal = journal

	if cfg.SocketPath != "" {
		l, err := StartCommitListener(cfg.SocketPath, mfs)
		if err != nil {
			stack.Close()
			return nil, fmt.Errorf("starting commit listener: %w", err)
		}
		stack.Sock = l
		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  listening for commits on %s\n", cfg.SocketPath)
		}
	}

	return stack, nil
}

// Close releases the commit listener, overlay, and journal; safe to call twice.
func (s *Stack) Close() {
	if s.Sock != nil {
		if err := s.Sock.Close(); err != nil {
			log.Error(err, "")
		}
	}
	if s.MFS != nil {
		s.MFS.Close()
	}
	if s.journal != nil {
		if err := s.journal.Close(); err != nil {
			log.Error(err, "")
		}
	}
}

// Serve mounts the snapshot and blocks until SIGINT/SIGTERM.
func Serve(cfg MountConfig) {
	stack, err := BuildStack(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "   error building mount stack: %v\n", err)
		os.Exit(1)
	}
	defer stack.Close()

	mfs := stack.MFS
	mutationMode := mfs != nil
	var rawFS fuse.RawFileSystem = stack.Raw
	var sockListener net.Listener = stack.Sock

	if cfg.NFS {
		serveNFS(cfg, rawFS, mfs, sockListener, !mutationMode)
		return
	}

	fuseOpts := cfg.FuseOpts
	if mutationMode {
		fuseOpts = strings.Replace(fuseOpts, "ro,", "rw,", 1)
		if !strings.Contains(fuseOpts, "rw") {
			fuseOpts = "rw,default_permissions"
		}
	}

	mountOpts := &fuse.MountOptions{
		Name:      "pxar-mount",
		Options:   strings.Split(fuseOpts, ","),
		EnableAcl: mfs != nil && mfs.acl.HasACLs(),
	}
	if mutationMode {
		mountOpts.ExtraCapabilities = fuse.CAP_WRITEBACK_CACHE
		mountOpts.MaxWrite = 1 << 20
		mountOpts.MaxReadAhead = 1 << 20
		mountOpts.MaxBackground = 64
	}

	server, err := fuse.NewServer(rawFS, cfg.MountPoint, mountOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if cfg.Verbose {
		mode := "mount"
		if cfg.InitMode {
			mode = "init"
		}
		fmt.Fprintf(os.Stderr, "  %s: serving at %s\n", mode, cfg.MountPoint)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, unix.SIGINT, unix.SIGTERM)
	go func() {
		<-sigCh

		if sockListener != nil {
			if err := sockListener.Close(); err != nil {
				log.Error(err, "")
			}
		}

		if err := server.Unmount(); err != nil {
			if err := unix.Unmount(cfg.MountPoint, unix.MNT_DETACH); err != nil {
				log.Error(err, "")
			}
		}

		<-sigCh
		os.Exit(1)
	}()

	server.Serve()

	closeMountState(mfs)
}

func serveNFS(cfg MountConfig, rawFS fuse.RawFileSystem, mfs *MutableFS, sockListener net.Listener, readOnly bool) {
	server, err := ServeNFSAsync(cfg, rawFS, readOnly)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  error creating NFS server: %v\n", err)
		os.Exit(1)
	}

	if cfg.Verbose {
		mode := "mount"
		if cfg.InitMode {
			mode = "init"
		}
		fmt.Fprintf(os.Stderr, "  %s: serving at %s via NFSv3\n", mode, cfg.MountPoint)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, unix.SIGINT, unix.SIGTERM)

	select {
	case <-sigCh:
		if sockListener != nil {
			if err := sockListener.Close(); err != nil {
				log.Error(err, "")
			}
		}
		if err := server.Close(); err != nil {
			log.Error(err, "")
		}
		select {
		case err := <-server.done:
			if err != nil {
				log.Error(err, "nfs serve")
			}
		case <-sigCh:
			os.Exit(1)
		}
	case err := <-server.done:
		if err != nil {
			log.Error(err, "nfs serve")
		}
		if err := server.Close(); err != nil {
			log.Error(err, "")
		}
	}

	closeMountState(mfs)
}

func closeMountState(mfs *MutableFS) {
	if mfs != nil {
		mfs.Close()
	}
	if globalCommitHub != nil {
		globalCommitHub.close()
	}
}
