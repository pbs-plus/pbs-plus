//go:build linux

package staging

import (
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
)

const dirName = "staging"

// Traversable, not listable: Dovecot drops to nobody and must reach its subdir.
const dirMode = 0o711

// Root returns the staging parent for a job, or "" to use the system temp dir.
// Dumps and mailboxes routinely outgrow /tmp, so they stage beside the datastore.
func Root(store string) string {
	if override := conf.Env.StagingDir; override != "" {
		return prepare(override)
	}
	if store == "" {
		return ""
	}
	info, err := cli.GetDatastoreInfo(store)
	if err != nil || info.Path == "" {
		log.Warn("staging falls back to the system temporary directory",
			"store", store, "error", err)
		return ""
	}
	base := filepath.Join(info.Path, ".pbs-plus")
	if prepare(base) == "" {
		return ""
	}
	return prepare(filepath.Join(base, dirName))
}

func prepare(dir string) string {
	if err := os.MkdirAll(dir, dirMode); err != nil {
		log.Warn("staging directory is unusable, falling back to the system temporary directory",
			"dir", dir, "error", err)
		return ""
	}
	info, err := os.Stat(dir)
	if err != nil {
		log.Warn("staging directory could not be inspected, falling back to the system temporary directory",
			"dir", dir, "error", err)
		return ""
	}
	if mode := info.Mode().Perm(); mode&0o111 != 0o111 {
		if err := os.Chmod(dir, mode|0o111); err != nil {
			log.Warn("staging directory is not traversable, falling back to the system temporary directory",
				"dir", dir, "error", err)
			return ""
		}
	}
	return dir
}
