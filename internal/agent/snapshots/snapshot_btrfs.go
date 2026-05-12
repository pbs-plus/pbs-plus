//go:build linux && cgo

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/containerd/btrfs/v2"
	"golang.org/x/sys/unix"
)

// BtrfsProvider creates snapshots using the BTRFS kernel ioctl
// (BTRFS_IOC_SNAP_CREATE_V2) via the containerd/btrfs library.
type BtrfsProvider struct{}

func (p *BtrfsProvider) Name() string { return "btrfs-ioctl" }

func (p *BtrfsProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	if !p.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("source path %q is not on a Btrfs subvolume", sourcePath)
	}

	tmpDir := os.TempDir()
	snapshotParent := filepath.Join(tmpDir, "pbs-plus-btrfs")
	if err := os.MkdirAll(snapshotParent, 0750); err != nil {
		return Snapshot{}, fmt.Errorf("btrfs: create snapshot dir: %w", err)
	}

	snapshotPath := filepath.Join(snapshotParent, jobID)
	timeStarted := time.Now()

	// Clean up any stale snapshot from a previous run.
	_ = os.RemoveAll(snapshotPath)

	if err := btrfs.SubvolSnapshot(snapshotPath, sourcePath, true); err != nil {
		return Snapshot{}, fmt.Errorf("btrfs snapshot create failed: %w", err)
	}

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		Handler:     p,
	}, nil
}

func (p *BtrfsProvider) DeleteSnapshot(snapshot Snapshot) error {
	if err := btrfs.SubvolDelete(snapshot.Path); err != nil {
		return fmt.Errorf("btrfs snapshot delete failed: %w", err)
	}
	return nil
}

func (p *BtrfsProvider) IsSupported(sourcePath string) bool {
	if runtime.GOOS != "linux" {
		return false
	}

	// Check if the path is on a btrfs filesystem.
	var st unix.Statfs_t
	if err := unix.Statfs(sourcePath, &st); err != nil {
		return false
	}
	if st.Type != btrfsSuperMagic {
		return false
	}

	// Verify it's actually a subvolume (not just on a btrfs filesystem).
	return btrfs.IsSubvolume(sourcePath) == nil
}

// btrfsSuperMagic is the BTRFS magic number.
const btrfsSuperMagic = 0x9123683E
