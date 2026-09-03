package pxarmount

type MountConfig struct {
	PBSStore      string
	Reader        any
	OrigPpxarDidx string
	BackingDir    string
	MountPoint    string
	SocketPath    string
	Namespace     string
	FuseOpts      string
	NFS           bool
	Verbose       bool
	InitMode      bool
	ACL           ACLConfig
}

// MarshalACL encodes POSIX ACL entries into the kernel binary format
// used by system.posix_acl_access and system.posix_acl_default.
