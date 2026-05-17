package pxarmount

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pxar/transfer"
	"golang.org/x/sys/unix"
)

// Serve builds the filesystem stack and runs the FUSE server.
// It blocks until the process receives SIGINT/SIGTERM.
func Serve(cfg MountConfig) {
	reader, _ := cfg.Reader.(*transfer.SplitArchiveReader)
	pxarFS, err := NewPxarFS(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error creating pxar FS: %v\n", err)
		os.Exit(1)
	}

	// Default backing dir for init mode.
	backingDir := cfg.BackingDir
	if backingDir == "" && cfg.InitMode {
		backingDir = cfg.MountPoint + ".backing"
	}

	mutationMode := backingDir != ""
	var rawFS fuse.RawFileSystem = pxarFS
	var mfs *MutableFS
	var sockListener net.Listener

	if mutationMode {
		if err := os.MkdirAll(backingDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error creating backing dir: %v\n", err)
			os.Exit(1)
		}

		// Open the SQLite journal.
		journalDir := filepath.Join(backingDir, JournalDir)
		journal, err := OpenJournal(journalDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error opening journal: %v\n", err)
			os.Exit(1)
		}
		defer func() { _ = journal.Close() }()

		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  mutation mode, journal in %s\n", journalDir)
		}

		mfs = NewMutableFS(pxarFS, journal, backingDir)

		origSnap := ParseOrigSnapshot(cfg.PBSStore, cfg.OrigPpxarDidx)
		if cfg.InitMode {
			origSnap = SnapshotRefForInit(cfg.Namespace)
		}

		mfs.SetSnapshotRef(origSnap)
		mfs.SetACLConfig(cfg.ACL)
		mfs.SetStorePaths(cfg.PBSStore, cfg.OrigPpxarDidx)
		mfs.SetVerbose(cfg.Verbose)

		if err := mfs.InitMutableRoot(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error initializing mutable root: %v\n", err)
			os.Exit(1)
		}

		// Reconcile orphan disk entries with journal on startup.
		if err := mfs.ReconcileMutableDir(); err != nil && cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  warning: reconcile error: %v\n", err)
		}

		// Apply default ownership and force-walk if requested.
		mfs.applyACLOwnership(backingDir)
		mfs.ForceACLOwnership()

		// Map root inode.
		mfs.mapInode(RootInode, "/")

		rawFS = mfs

		if cfg.SocketPath != "" {
			l, err := StartCommitListener(cfg.SocketPath, mfs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ✗ error starting socket listener: %v\n", err)
				os.Exit(1)
			}
			sockListener = l
			if cfg.Verbose {
				fmt.Fprintf(os.Stderr, "  listening for commits on %s\n", cfg.SocketPath)
			}
		}
	}

	fuseOpts := cfg.FuseOpts
	if mutationMode {
		fuseOpts = strings.Replace(fuseOpts, "ro,", "rw,", 1)
		if !strings.Contains(fuseOpts, "rw") {
			fuseOpts = "rw,default_permissions"
		}
	}

	server, err := fuse.NewServer(rawFS, cfg.MountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: strings.Split(fuseOpts, ","),
	})
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

		// Stop accepting new commit connections.
		if sockListener != nil {
			_ = sockListener.Close()
		}

		// Unmount FUSE.
		if err := server.Unmount(); err != nil {
			_ = unix.Unmount(cfg.MountPoint, unix.MNT_DETACH)
		}

		// Second signal: force exit.
		<-sigCh
		os.Exit(1)
	}()

	server.Serve()

	// Cleanup after Serve() returns.
	if mfs != nil {
		mfs.Close()
	}
}
