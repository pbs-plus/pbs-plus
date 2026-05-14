package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/pxarmount"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "commit":
			pxarmount.RunCommitSubcommand()
			return
		case "init":
			pxarmount.RunInitSubcommand()
			return
		}
	}
	runMountSubcommand()
}

// runMountSubcommand is the default mount mode — mounts an existing pxar snapshot.
func runMountSubcommand() {
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
	aclOwner := flag.Int("acl-owner", 0, "Default owner UID for new files/dirs (0 = inherit)")
	aclGroup := flag.Int("acl-group", 0, "Default group GID for new files/dirs (0 = inherit)")
	forceAclOwner := flag.Bool("force-acl-owner", false, "Force set owner on all existing files at mount")
	forceAclGroup := flag.Bool("force-acl-group", false, "Force set group on all existing files at mount")

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

	// Open the chunk store and build the archive reader.
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

	pxarmount.Serve(pxarmount.MountConfig{
		PBSStore:      *pbsStore,
		Reader:        reader,
		OrigPpxarDidx: *ppxarDidx,
		BackingDir:    *passthrough,
		MountPoint:    mountPoint,
		SocketPath:    *socketPath,
		FuseOpts:      *fuseOpts,
		Verbose:       *verbose,
		ACL: pxarmount.ACLConfig{
			OwnerUID:   *aclOwner,
			OwnerGID:   *aclGroup,
			ForceOwner: *forceAclOwner,
			ForceGroup: *forceAclGroup,
		},
	})
}
