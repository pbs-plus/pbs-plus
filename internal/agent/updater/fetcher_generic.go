// updater/fetcher_generic.go
//go:build !linux && !windows

package updater

import "github.com/pbs-plus/pbs-plus/internal/store/constants"

func (u *UpdateFetcher) Init() error {
	if u.currentVersion == "" {
		u.currentVersion = constants.Version
	}
	// No OS-specific cleanup on generic platforms.
	return nil
}
