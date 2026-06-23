//go:build !linux && !windows

package updater

func cleanUp() error {
	return nil
}
