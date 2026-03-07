//go:build unix

package updater

func restartCallback(_ Config) bool {
	return false
}
