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
	return prepare(filepath.Join(info.Path, ".pbs-plus", dirName))
}

func prepare(dir string) string {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		log.Warn("staging directory is unusable, falling back to the system temporary directory",
			"dir", dir, "error", err)
		return ""
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		log.Warn("staging directory could not be secured, falling back to the system temporary directory",
			"dir", dir, "error", err)
		return ""
	}
	return dir
}
