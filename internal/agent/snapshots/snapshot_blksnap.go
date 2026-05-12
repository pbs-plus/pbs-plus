//go:build linux

package snapshots

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/go-blksnap"
	"golang.org/x/sys/unix"
)

// BlksnapProvider creates block-level snapshots using the veeamblksnap
// kernel module. This is the preferred mechanism for ext4 and XFS
// filesystems (Veeam-style compatibility).
type BlksnapProvider struct {
	session    *blksnap.Session
	mountPoint string
}

func (p *BlksnapProvider) Name() string { return "blksnap" }

func (p *BlksnapProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	if !p.IsSupported(sourcePath) {
		return Snapshot{}, fmt.Errorf("blksnap: source path %q is not on a supported block device", sourcePath)
	}

	dev, err := blockDeviceForPath(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: find block device: %w", err)
	}

	workDir := filepath.Join(os.TempDir(), "pbs-plus-blksnap", jobID)
	if err := os.MkdirAll(workDir, 0750); err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: create work dir: %w", err)
	}

	diffStorage := filepath.Join(workDir, "diff_storage")
	const diffStorageLimit = 1 << 30 // 1 GiB

	session, err := blksnap.CreateSession(
		[]string{dev},
		diffStorage,
		diffStorageLimit,
	)
	if err != nil {
		_ = os.RemoveAll(workDir)
		return Snapshot{}, fmt.Errorf("blksnap: create session: %w", err)
	}

	var cleanupDone bool
	defer func() {
		if !cleanupDone {
			_ = session.Close()
			_ = os.RemoveAll(workDir)
		}
	}()

	// Get the snapshot image device.
	cbt, err := session.CBTHandle(dev)
	if err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: get CBT handle: %w", err)
	}
	imageDev, err := cbt.Image()
	_ = cbt.Close()
	if err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: get image device: %w", err)
	}

	// Mount the snapshot image read-only.
	mountPoint := filepath.Join(workDir, "mnt")
	if err := os.MkdirAll(mountPoint, 0750); err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: create mount dir: %w", err)
	}

	fsType, _ := detectFilesystem(sourcePath)
	if err := unix.Mount(imageDev, mountPoint, fsType, unix.MS_RDONLY, ""); err != nil {
		return Snapshot{}, fmt.Errorf("blksnap: mount snapshot: %w", err)
	}

	p.session = session
	p.mountPoint = mountPoint
	cleanupDone = true

	// Compute the subpath: the portion of sourcePath inside the mount.
	mountEntry, _ := resolveMountPoint(sourcePath)
	subPath := strings.TrimPrefix(sourcePath, mountEntry.mountPoint)
	subPath = strings.TrimPrefix(subPath, "/")

	timeStarted := time.Now()
	return Snapshot{
		Path:        mountPoint,
		TimeStarted: timeStarted,
		SourcePath:  sourcePath,
		SubPath:     subPath,
		Handler:     p,
	}, nil
}

func (p *BlksnapProvider) DeleteSnapshot(snapshot Snapshot) error {
	var errs []string

	if p.mountPoint != "" {
		if err := unix.Unmount(p.mountPoint, 0); err != nil {
			errs = append(errs, fmt.Sprintf("unmount: %v", err))
		}
	}

	if p.session != nil {
		if err := p.session.Close(); err != nil {
			errs = append(errs, fmt.Sprintf("session close: %v", err))
		}
		p.session = nil
	}

	// Clean up work directory (parent of mount point).
	if p.mountPoint != "" {
		workDir := filepath.Dir(p.mountPoint)
		_ = os.RemoveAll(workDir)
		p.mountPoint = ""
	}

	if len(errs) > 0 {
		return fmt.Errorf("blksnap cleanup: %s", strings.Join(errs, "; "))
	}
	return nil
}

func (p *BlksnapProvider) IsSupported(sourcePath string) bool {
	// Check if the blksnap kernel module is loaded.
	if _, err := os.Stat(blksnap.ControlDevice); err != nil {
		return false
	}

	// Must be on a block device (not a network fs, tmpfs, etc.).
	dev, err := blockDeviceForPath(sourcePath)
	if err != nil || dev == "" {
		return false
	}

	// Only support ext4 and XFS.
	fsType, err := detectFilesystem(sourcePath)
	if err != nil {
		return false
	}
	switch fsType {
	case "ext4", "xfs":
		return true
	}
	return false
}
