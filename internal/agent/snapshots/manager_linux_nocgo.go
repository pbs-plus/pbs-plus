//go:build linux && !cgo

package snapshots

func initProviders() map[string]providerChain {
	blksnapP := &BlksnapProvider{}
	zfsP := &ZFSProvider{}
	vssP := &VSSProvider{}

	chains := map[string]providerChain{
		"ext4": {blksnapP},
		"xfs":  {blksnapP},
		// BTRFS ioctl provider requires cgo (containerd/btrfs/v2).
		// On CGO-disabled builds (e.g. Docker), BTRFS falls through
		// to ErrNoSnapshotSupport → file-level direct mode.
		"btrfs": nil,
		"zfs":   {zfsP},
		"ntfs":  {vssP},
		"refs":  {vssP},
		"fat32": nil,
		"exfat": nil,
		"hfs+":  nil,
	}

	for fs, chain := range chains {
		var filtered providerChain
		for _, p := range chain {
			if p != nil {
				filtered = append(filtered, p)
			}
		}
		chains[fs] = filtered
	}

	return chains
}
