//go:build !linux

package snapshots

func initProviders() map[string]providerChain {
	vssP := &VSSProvider{}

	chains := map[string]providerChain{
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
