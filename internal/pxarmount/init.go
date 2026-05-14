package pxarmount

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// SnapshotRefForInit returns a snapshotRef suitable for init mode.
// Only the namespace and default backup type are set — backup ID, time,
// and archive name are derived from the commit request at commit time.
func SnapshotRefForInit(namespace string) snapshotRef {
	return snapshotRef{
		Namespace:  namespace,
		BackupType: "host",
	}
}

// RunInitSubcommand is the CLI entry point for `pxar-mount init`.
// It creates a writable FUSE mount backed by an empty pxar filesystem.
// The first commit creates a new PBS snapshot from scratch.
func RunInitSubcommand() {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	pbsStore := fs.String("pbs-store", "", "PBS datastore root path (required)")
	socketPath := fs.String("socket", "", "Unix socket path for commit commands (required)")
	namespace := fs.String("namespace", "", "PBS namespace")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")
	fuseOpts := fs.String("options", "rw,default_permissions", "FUSE mount options")

	fs.Parse(os.Args[2:])

	if *pbsStore == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount init --pbs-store <path> --socket <path> [--namespace <ns>] [--verbose] <mountpoint>\n\n")
		fmt.Fprintf(os.Stderr, "Creates a writable mount that, on commit, produces a new PBS snapshot.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}
	mountPoint := args[0]

	backingDir := mountPoint + ".backing"
	if err := os.MkdirAll(backingDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error creating backing dir: %v\n", err)
		os.Exit(1)
	}

	pxarFS, err := NewPxarFS(nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error creating empty pxar FS: %v\n", err)
		os.Exit(1)
	}

	mutationsDir := filepath.Join(backingDir, TransactionsDir)
	tl, err := OpenTransactionLog(mutationsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error opening transaction log: %v\n", err)
		os.Exit(1)
	}
	defer tl.Close()

	if *verbose {
		fmt.Fprintf(os.Stderr, "  init: transactions in %s\n", mutationsDir)
	}

	ptFS := NewPassthroughFS(pxarFS, backingDir, *pbsStore, "", true, tl)
	ptFS.SetSnapshotRef(SnapshotRefForInit(*namespace))

	if err := ptFS.InitPassthroughRoot(); err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error initializing passthrough root: %v\n", err)
		os.Exit(1)
	}

	var sockListener net.Listener
	if *socketPath != "" {
		l, _, err := ptFS.StartSocketListener(*socketPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  ✗ error starting socket listener: %v\n", err)
			os.Exit(1)
		}
		sockListener = l
		if *verbose {
			fmt.Fprintf(os.Stderr, "  init: listening for commits on %s\n", *socketPath)
		}
	}

	server, err := fuse.NewServer(ptFS, mountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: strings.Split(*fuseOpts, ","),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  ✗ error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "  init: serving at %s\n", mountPoint)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		if sockListener != nil {
			_ = sockListener.Close()
		}
		ptFS.Close()
		_ = server.Unmount()
	}()

	server.Serve()
}
