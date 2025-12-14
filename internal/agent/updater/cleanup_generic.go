//go:build !linux && !windows

package updater

func cleanUp() error {
	// No OS-specific cleanup on generic platforms.
	return nil
}
