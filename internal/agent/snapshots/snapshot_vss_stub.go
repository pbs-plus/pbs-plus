//go:build !windows

package snapshots

// VSSProvider is a no-op on non-Windows platforms.
type VSSProvider struct{}

func (p *VSSProvider) Name() string { return "vss" }

func (p *VSSProvider) CreateSnapshot(jobID, sourcePath string) (Snapshot, error) {
	return Snapshot{}, ErrNoSnapshotSupport
}

func (p *VSSProvider) DeleteSnapshot(snapshot Snapshot) error { return nil }

func (p *VSSProvider) IsSupported(sourcePath string) bool { return false }
