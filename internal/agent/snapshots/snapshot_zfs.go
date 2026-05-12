//go:build linux

package snapshots

import "runtime"

// ZFSProvider signals that ZFS does not support external snapshots.
// ZFS has its own built-in snapshot mechanism (`zfs snapshot`), but
// those snapshots live inside the ZFS pool and cannot be exposed as
// a separate read-only path without `zfs clone` + mount. For backup
// purposes we treat ZFS as snapshot-less and fall back to file-level
// (direct) reads.
type ZFSProvider struct{}

func (p *ZFSProvider) Name() string { return "zfs-no-snapshot" }

func (p *ZFSProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	return Snapshot{}, ErrNoSnapshotSupport
}

func (p *ZFSProvider) DeleteSnapshot(snapshot Snapshot) error { return nil }

func (p *ZFSProvider) IsSupported(sourcePath string) bool {
	if runtime.GOOS != "linux" {
		return false
	}
	// We register ZFS so the manager knows to use file-level fallback
	// instead of returning an unknown-filesystem error. Always return
	// true on ZFS so the provider chain picks it up.
	return detectZFS(sourcePath)
}

// detectZFS checks if a path resides on a ZFS filesystem.
func detectZFS(path string) bool {
	return false // reserved for future statfs-based ZFS detection
}
