package pxarmount

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pxar/transfer"
)

// MountConfig holds all parameters needed to start a pxar-mount FUSE server.
type MountConfig struct {
	// PBSStore is the root path of the PBS datastore on disk.
	PBSStore string

	// Reader is the pxar archive reader. Nil means init mode (empty mount).
	Reader *transfer.SplitArchiveReader

	// OrigPpxarDidx is the path to the original .ppxar.didx file.
	// Empty for init mode. Used to derive snapshot ref and dedup index.
	OrigPpxarDidx string

	// BackingDir is the passthrough backing directory.
	// If empty, no mutation mode (read-only mount).
	// For init mode, defaults to MountPoint + ".backing" if empty.
	BackingDir string

	// MountPoint is where the FUSE filesystem is attached.
	MountPoint string

	// SocketPath is the Unix domain socket for commit commands.
	SocketPath string

	// Namespace is the PBS namespace. Used for init mode and auto-created on commit.
	Namespace string

	// FuseOpts are comma-separated FUSE mount options.
	FuseOpts string

	// Verbose enables diagnostic output.
	Verbose bool

	// InitMode creates a writable mount with no base snapshot.
	// Reader must be nil. On commit, a new PBS snapshot is created from scratch.
	InitMode bool

	// ACL configures default ownership and force-set behavior.
	ACL ACLConfig
}

// EnsureNamespaceDir creates the namespace directory structure on the PBS
// datastore if it doesn't already exist. Follows the PBS convention:
//
//	<datastore>/ns/<component>/ns/<component>/...
func EnsureNamespaceDir(pbsStore, namespace string) error {
	if namespace == "" {
		return nil
	}
	path := pbsStore
	for comp := range strings.SplitSeq(namespace, "/") {
		if comp == "" {
			continue
		}
		path = filepath.Join(path, "ns", comp)
	}
	fi, err := os.Stat(path)
	if err == nil && fi.IsDir() {
		return nil
	}
	return os.MkdirAll(path, 0o755)
}

// Serve builds the filesystem stack and runs the FUSE server.
// It blocks until the process receives SIGINT/SIGTERM.
func Serve(cfg MountConfig) {
	pxarFS, err := NewPxarFS(cfg.Reader)
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
	var ptFSClose func()
	var sockListener net.Listener

	if mutationMode {
		if err := os.MkdirAll(backingDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error creating backing dir: %v\n", err)
			os.Exit(1)
		}

		origSnap := ParseOrigSnapshot(cfg.PBSStore, cfg.OrigPpxarDidx)
		if cfg.InitMode {
			origSnap = SnapshotRefForInit(cfg.Namespace)
		}

		mutationsDir := filepath.Join(backingDir, TransactionsDir)
		tl, err := OpenTransactionLog(mutationsDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error opening transaction log: %v\n", err)
			os.Exit(1)
		}
		defer tl.Close()

		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  mutation mode, transactions in %s\n", mutationsDir)
		}

		ptFS := NewPassthroughFS(pxarFS, backingDir, cfg.PBSStore, cfg.OrigPpxarDidx, true, tl)
		ptFS.SetSnapshotRef(origSnap)
		ptFS.SetACLConfig(cfg.ACL)

		rawFS = ptFS
		ptFSClose = ptFS.Close

		if err := ptFS.InitPassthroughRoot(); err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error initializing passthrough root: %v\n", err)
			os.Exit(1)
		}

		// Apply default ownership on root and force-walk if requested.
		ptFS.applyACLOwnership(backingDir, true)
		ptFS.ForceACLOwnership()

		if cfg.SocketPath != "" {
			l, _, err := ptFS.StartSocketListener(cfg.SocketPath)
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		if sockListener != nil {
			_ = sockListener.Close()
		}
		if ptFSClose != nil {
			ptFSClose()
		}
		_ = server.Unmount()
	}()

	server.Serve()
}
