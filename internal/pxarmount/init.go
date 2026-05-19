package pxarmount

import (
	"flag"
	"fmt"
	"os"
)

// SnapshotRefForInit returns a snapshotRef suitable for init mode.
func SnapshotRefForInit(namespace string) snapshotRef {
	return snapshotRef{
		Namespace:  namespace,
		BackupType: "host",
	}
}

// RunInitSubcommand is the CLI entry point for `pxar-mount init`.
func RunInitSubcommand() {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	pbsStore := fs.String("pbs-store", "", "PBS datastore root path (required)")
	socketPath := fs.String("socket", "", "Unix socket path for commit commands (required)")
	namespace := fs.String("namespace", "", "PBS namespace")
	passthrough := fs.String("passthrough", "", "Backing directory for write passthrough")
	verbose := fs.Bool("verbose", false, "Enable verbose logging")
	fuseOpts := fs.String("options", "rw,default_permissions", "FUSE mount options")
	aclOwner := fs.Int("acl-owner", 0, "Default owner UID for new files/dirs (0 = inherit)")
	aclGroup := fs.Int("acl-group", 0, "Default group GID for new files/dirs (0 = inherit)")
	forceAclOwner := fs.Bool("force-acl-owner", false, "Force set owner on all existing files at mount")
	forceAclGroup := fs.Bool("force-acl-group", false, "Force set group on all existing files at mount")
	aclSpec := fs.String("acl-spec", "", "POSIX ACL spec string (setfacl-style) served as virtual xattrs")
	defaultAclSpec := fs.String("default-acl-spec", "", "Default POSIX ACL spec string served as virtual xattrs")

	fs.Parse(os.Args[2:]) //nolint:errcheck // ExitOnError set, calls os.Exit on failure

	if *pbsStore == "" {
		fmt.Fprintf(os.Stderr, "Usage: pxar-mount init --pbs-store <path> --socket <path> [--passthrough <dir>] [--namespace <ns>] [--verbose] <mountpoint>\n\n")
		fmt.Fprintf(os.Stderr, "Creates a writable mount that, on commit, produces a new PBS snapshot.\n")
		fs.PrintDefaults()
		os.Exit(1)
	}

	args := fs.Args()
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "Error: mountpoint required\n")
		os.Exit(1)
	}

	Serve(MountConfig{
		PBSStore:   *pbsStore,
		BackingDir: *passthrough,
		MountPoint: args[0],
		SocketPath: *socketPath,
		Namespace:  *namespace,
		FuseOpts:   *fuseOpts,
		Verbose:    *verbose,
		InitMode:   true,
		ACL:        BuildACLConfig(*aclOwner, *aclGroup, *forceAclOwner, *forceAclGroup, *aclSpec, *defaultAclSpec),
	})
}
