//go:build windows

package updater

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"golang.org/x/sys/windows/svc/mgr"
)

func cleanUp() error {
	const svcName = "PBSPlusUpdater"

	m, err := mgr.Connect()
	if err != nil {
		log.Error(err, "failed to connect to Windows SCM")
		return nil
	}
	defer m.Disconnect()

	s, err := m.OpenService(svcName)
	if err == nil {
		_, _ = s.Control(1)
		_ = s.Delete()

		cfg, cfgErr := s.Config()
		binPath := ""
		if cfgErr == nil {
			binPath = cfg.BinaryPathName
		}
		_ = s.Close()

		if binPath != "" {
			binPath = strings.Trim(binPath, `"`)
			if fi, statErr := os.Stat(binPath); statErr == nil && !fi.IsDir() {
				if rmErr := os.Remove(binPath); rmErr != nil {
					log.Error(err,

						"failed to remove updater binary", "path", binPath)

				} else {
					log.Info("removed updater binary", "path", binPath)

				}
			}
		} else {
			candidates := []string{
				filepath.Join(os.Getenv("ProgramFiles"), "PBS Plus", "PBSPlusUpdater.exe"),
				filepath.Join(os.Getenv("ProgramFiles(x86)"), "PBS Plus", "PBSPlusUpdater.exe"),
			}
			for _, c := range candidates {
				if c == "" {
					continue
				}
				if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
					_ = os.Remove(c)
				}
			}
		}
	}

	return nil
}
