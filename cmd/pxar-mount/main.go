package main

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
	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "commit" {
		pxarmount.RunCommitSubcommand()
		return
	}

	pbsStore := flag.String("pbs-store", "", "PBS datastore root path")
	mpxarDidx := flag.String("mpxar-didx", "", "Path to metadata dynamic index (.mpxar.didx)")
	ppxarDidx := flag.String("ppxar-didx", "", "Path to payload or unified dynamic index (.ppxar.didx)")
	keyfile := flag.String("keyfile", "", "Path to encryption key (accepted for CLI compat)")
	verifyChunks := flag.Bool("verify-chunks", false, "Verify chunk SHA256 on read (accepted for CLI compat)")
	cacheMB := flag.Int("cache-size", 256, "Cache size in MB (accepted for CLI compat)")
	fuseOpts := flag.String("options", "ro,default_permissions", "FUSE mount options")
	passthrough := flag.String("passthrough", "", "Backing directory for write passthrough")
	socketPath := flag.String("socket", "", "Unix socket path for commit commands")
	verbose := flag.Bool("verbose", false, "Enable verbose logging")

	flag.Parse()

	if *pbsStore == "" || *ppxarDidx == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount --pbs-store <path> --ppxar-didx <path> [--mpxar-didx <path>] [--passthrough <dir>] [--socket <path>] [--verbose] <mountpoint>\n")
		os.Exit(1)
	}

	if *passthrough == "" && *socketPath != "" {
		fmt.Fprintf(os.Stderr, "Error: --socket requires --passthrough\n")
		os.Exit(1)
	}

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}
	mountPoint := args[0]

	_, _, _ = keyfile, verifyChunks, cacheMB

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: datastore=%s metadata=%s payload=%s mount=%s\n",
			*pbsStore, *mpxarDidx, *ppxarDidx, mountPoint)
	}

	store, err := datastore.NewChunkStore(*pbsStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening chunk store: %v\n", err)
		os.Exit(1)
	}
	source := datastore.NewChunkStoreSource(store)

	var reader *transfer.SplitArchiveReader
	isSplit := *mpxarDidx != ""

	if isSplit {
		metaData, err := os.ReadFile(*mpxarDidx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading metadata index: %v\n", err)
			os.Exit(1)
		}
		payloadData, err := os.ReadFile(*ppxarDidx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading payload index: %v\n", err)
			os.Exit(1)
		}
		reader, err = transfer.NewSplitArchiveReader(metaData, payloadData, source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating split archive reader: %v\n", err)
			os.Exit(1)
		}
	} else {
		fmt.Fprintf(os.Stderr, "Error: non-split (unified) .pxar.didx archives not yet supported\n")
		os.Exit(1)
	}
	defer reader.Close()

	pxarFS, err := pxarmount.NewPxarFS(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating pxar FS: %v\n", err)
		os.Exit(1)
	}

	var rawFS fuse.RawFileSystem = pxarFS
	var sockListener net.Listener

	var ptFSClose func()

	if *passthrough != "" {
		ptInfo, err := os.Stat(*passthrough)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: passthrough directory not accessible: %v\n", err)
			os.Exit(1)
		}
		if !ptInfo.IsDir() {
			fmt.Fprintf(os.Stderr, "Error: passthrough path is not a directory\n")
			os.Exit(1)
		}

		origSnap := pxarmount.ParseOrigSnapshot(*pbsStore, *ppxarDidx)

		mutationsDir := filepath.Join(*passthrough, pxarmount.TransactionsDir)
		tl, err := pxarmount.OpenTransactionLog(mutationsDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening transaction log: %v\n", err)
			os.Exit(1)
		}
		defer tl.Close()

		if *verbose {
			fmt.Fprintf(os.Stderr, "pxar-mount: mutation mode enabled, transactions in %s\n", mutationsDir)
		}

		ptFS := pxarmount.NewPassthroughFS(pxarFS, *passthrough, *pbsStore, *ppxarDidx, true, tl)
		// Set the snapshot ref
		ptFS.SetSnapshotRef(origSnap)

		rawFS = ptFS
		ptFSClose = ptFS.Close

		if err := ptFS.InitPassthroughRoot(); err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing passthrough root: %v\n", err)
			os.Exit(1)
		}

		if *socketPath != "" {
			l, _, err := ptFS.StartSocketListener(*socketPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error starting socket listener: %v\n", err)
				os.Exit(1)
			}
			sockListener = l
			if *verbose {
				fmt.Fprintf(os.Stderr, "pxar-mount: listening for commits on %s\n", *socketPath)
			}
		}

		*fuseOpts = strings.Replace(*fuseOpts, "ro,", "rw,", 1)
		if !strings.Contains(*fuseOpts, "rw") {
			*fuseOpts = "rw,default_permissions"
		}

		if *verbose {
			fmt.Fprintf(os.Stderr, "pxar-mount: passthrough mode, backing dir=%s\n", *passthrough)
		}
	}

	server, err := fuse.NewServer(rawFS, mountPoint, &fuse.MountOptions{
		Name:    "pxar-mount",
		Options: strings.Split(*fuseOpts, ","),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating FUSE server: %v\n", err)
		os.Exit(1)
	}

	if *verbose {
		fmt.Fprintf(os.Stderr, "pxar-mount: serving at %s\n", mountPoint)
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
