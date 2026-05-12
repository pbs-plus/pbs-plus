//go:build linux

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/dennwc/btrfs"
	"golang.org/x/sys/unix"
)

// BtrfsProvider creates snapshots using the BTRFS kernel ioctl
// (BTRFS_IOC_SNAP_CREATE_V2) via the pure-Go dennwc/btrfs library.
type BtrfsProvider struct{}

func (p *BtrfsProvider) Name() string { return "btrfs-ioctl" }

func (p *BtrfsProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	subvolRoot, err := resolveBtrfsSubvolume(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("btrfs: %w", err)
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

	if err := btrfs.SnapshotSubVolume(subvolRoot, snapshotPath, true); err != nil {
		return Snapshot{}, fmt.Errorf("btrfs snapshot create failed: %w", err)
	}

	// Compute the subpath: the portion of sourcePath inside the subvolume.
	subPath := strings.TrimPrefix(sourcePath, subvolRoot)
	subPath = strings.TrimPrefix(subPath, "/")

	return Snapshot{
		Path:        snapshotPath,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		SubPath:     subPath,
		Handler:     p,
	}, nil
}

func (p *BtrfsProvider) DeleteSnapshot(snapshot Snapshot) error {
	if err := btrfs.DeleteSubVolume(snapshot.Path); err != nil {
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

	// Walk up to find the containing subvolume.
	_, err := resolveBtrfsSubvolume(sourcePath)
	return err == nil
}

// btrfsSuperMagic is the BTRFS magic number.
const btrfsSuperMagic = 0x9123683E

// resolveBtrfsSubvolume walks up from path to find the BTRFS subvolume
// root. Returns the subvolume path or an error if none is found.
func resolveBtrfsSubvolume(path string) (string, error) {
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve absolute path: %w", err)
		}
		path = abs
	}

	for {
		isSubvol, err := btrfs.IsSubVolume(path)
		if err != nil {
			return "", fmt.Errorf("check subvolume at %s: %w", path, err)
		}
		if isSubvol {
			return path, nil
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("no btrfs subvolume found above %s", path)
		}
		path = parent
	}
}
