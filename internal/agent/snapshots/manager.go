package snapshots

import (
	"fmt"
	"strings"
)

// providerChain is a prioritized list of providers for a given filesystem type.
type providerChain []SnapshotProvider

// Manager orchestrates snapshot creation across filesystems using a
// priority-based provider chain.
//
// For each filesystem type, providers are tried in order. The first
// provider whose IsSupported() returns true handles the snapshot.
type Manager struct {
	chains map[string]providerChain
}

// DefaultManager is the global snapshot manager instance.
var DefaultManager = &Manager{
	chains: initProviders(),
}

// CreateSnapshot detects the filesystem and delegates to the first
// supported provider in the chain.
func (m *Manager) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	fsType, err := detectFilesystem(sourcePath)
	if err != nil {
		return Snapshot{}, fmt.Errorf("snapshot: detect filesystem: %w", err)
	}

	fsType = normalizeFilesystemType(fsType)
	chain, exists := m.chains[fsType]
	if !exists {
		return Snapshot{}, fmt.Errorf("snapshot: unknown filesystem type: %s", fsType)
	}
	if len(chain) == 0 {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrNoSnapshotSupport, fsType)
	}

	for _, provider := range chain {
		if provider.IsSupported(sourcePath) {
			snap, err := provider.CreateSnapshot(jobID, sourcePath)
			if err != nil {
				return Snapshot{}, fmt.Errorf("snapshot: provider %q: %w", provider.Name(), err)
			}
			return snap, nil
		}
	}

	return Snapshot{}, fmt.Errorf("%w: no provider available for %s", ErrNoSnapshotSupport, fsType)
}

// normalizeFilesystemType normalizes common filesystem type variations
// (e.g. fuseblk → ntfs on Linux).
func normalizeFilesystemType(fsType string) string {
	fsType = strings.ToLower(fsType)
	switch fsType {
	case "fuseblk":
		return "ntfs"
	case "ext3", "ext2":
		return "ext4"
	default:
		return fsType
	}
}
