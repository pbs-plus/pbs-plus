//go:build windows

package updater

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows/svc/mgr"
)

func cleanUp() error {
	const svcName = "PBSPlusUpdater"

	m, err := mgr.Connect()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to connect to Windows SCM").Write()
		return nil
	}
	defer m.Disconnect()

	s, err := m.OpenService(svcName)
	if err == nil {
		_, _ = s.Control(1) // svc.Stop = 1
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
					syslog.L.Error(err).
						WithField("path", binPath).
						WithMessage("failed to remove updater binary").
						Write()
				} else {
					syslog.L.Info().
						WithField("path", binPath).
						WithMessage("removed updater binary").
						Write()
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
