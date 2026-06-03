//go:build linux

package notification

import (
	"embed"
	"os"
	"path/filepath"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

//go:embed templates/default/*.hbs
var templateFS embed.FS

var (
	templateOnce sync.Once
)

// InstallTemplates copies embedded notification templates to the PBS vendor
// template directory if they don't already exist. This runs once on startup.
func InstallTemplates() {
	templateOnce.Do(func() {
		entries, err := templateFS.ReadDir("templates/default")
		if err != nil {
			syslog.L.Error(err).WithMessage("failed to read embedded notification templates").Write()
			return
		}

		// Ensure vendor template directory exists
		if err := os.MkdirAll(VendorTemplateDir, 0755); err != nil {
			syslog.L.Error(err).WithMessage("failed to create template directory").Write()
			return
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			destPath := filepath.Join(VendorTemplateDir, entry.Name())

			// Don't overwrite existing files (user may have customized them)
			if _, err := os.Stat(destPath); err == nil {
				continue
			}

			data, err := templateFS.ReadFile(filepath.Join("templates", "default", entry.Name()))
			if err != nil {
				syslog.L.Error(err).WithField("template", entry.Name()).WithMessage("failed to read embedded template").Write()
				continue
			}

			if err := os.WriteFile(destPath, data, 0644); err != nil {
				syslog.L.Error(err).WithField("path", destPath).WithMessage("failed to install notification template").Write()
				continue
			}
		}

	})
}
