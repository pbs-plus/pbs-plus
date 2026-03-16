//go:build unix

package updater

func restartCallback(cfg Config) bool {
	if cfg.Exit != nil {
		cfg.Exit(nil)
	}
	return false
}
