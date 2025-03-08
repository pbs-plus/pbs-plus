package snapshots

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ceph/go-ceph/cephfs"
)

type CephfsSnapshotHandler struct {
	mount *cephfs.MountInfo
}

// NewCephfsSnapshotHandler initializes a new CephfsSnapshotHandler
func NewCephfsSnapshotHandler() (*CephfsSnapshotHandler, error) {
	mount, err := cephfs.CreateMount()
	if err != nil {
		return nil, fmt.Errorf("failed to create CephFS mount: %w", err)
	}

	if err := mount.ReadDefaultConfigFile(); err != nil {
		return nil, fmt.Errorf("failed to read CephFS config: %w", err)
	}

	if err := mount.Mount(); err != nil {
		return nil, fmt.Errorf("failed to mount CephFS: %w", err)
	}

	return &CephfsSnapshotHandler{mount: mount}, nil
}

// CreateSnapshot creates a snapshot for the given source path
func (c *CephfsSnapshotHandler) CreateSnapshot(ctx context.Context, jobId string, sourcePath string) (Snapshot, error) {
	if sourcePath == "" {
		return Snapshot{}, errors.New("empty source path")
	}

	// Ensure the source path is valid
	if err := c.validatePath(sourcePath); err != nil {
		return Snapshot{}, fmt.Errorf("source path %q does not exist: %w", sourcePath, err)
	}

	// Generate snapshot name
	snapshotName := fmt.Sprintf("snapshot_%s_%d", jobId, time.Now().Unix())

	// Create the snapshot using MDS command
	args := [][]byte{
		[]byte("fs"),
		[]byte("snapshot"),
		[]byte("create"),
		[]byte(sourcePath),
		[]byte(snapshotName),
	}
	if _, _, err := c.mount.MdsCommand("", args); err != nil {
		return Snapshot{}, fmt.Errorf("failed to create CephFS snapshot: %w", err)
	}

	return Snapshot{
		Path:        filepath.Join(sourcePath, ".snap", snapshotName),
		TimeStarted: time.Now(),
		SourcePath:  sourcePath,
		Handler:     c,
	}, nil
}

// DeleteSnapshot deletes the specified snapshot
func (c *CephfsSnapshotHandler) DeleteSnapshot(snapshot Snapshot) error {
	// Extract the snapshot name from the snapshot path
	snapshotName := filepath.Base(snapshot.Path)

	// Delete the snapshot using MDS command
	args := [][]byte{
		[]byte("fs"),
		[]byte("snapshot"),
		[]byte("rm"),
		[]byte(snapshot.SourcePath),
		[]byte(snapshotName),
	}
	if _, _, err := c.mount.MdsCommand("", args); err != nil {
		return fmt.Errorf("failed to delete CephFS snapshot: %w", err)
	}

	return nil
}

// IsSupported checks if the source path is supported by CephFS
func (c *CephfsSnapshotHandler) IsSupported(sourcePath string) bool {
	// Check if the source path exists in the CephFS mount
	return c.validatePath(sourcePath) == nil
}

// validatePath checks if a given path exists in the CephFS mount
func (c *CephfsSnapshotHandler) validatePath(path string) error {
	_, err := c.mount.Statx(path, 0, 0) // Use 0 for `want` and `flags` as a fallback
	return err
}

// Close releases the CephFS mount
func (c *CephfsSnapshotHandler) Close() error {
	if c.mount != nil {
		if err := c.mount.Unmount(); err != nil {
			return fmt.Errorf("failed to unmount CephFS: %w", err)
		}
		c.mount.Release()
	}
	return nil
}
