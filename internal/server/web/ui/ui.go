// Package ui defines the ExtJS additions injected into Proxmox Backup Server.
package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

// Render returns the generated JavaScript definitions in dependency order.
func Render() []byte {
	items := append(models, mtfModels...)
	items = append(items, scriptSelector, scriptEditWindow, scriptPanel)
	items = append(items, selectors...)
	items = append(items, pathSelectors...)
	items = append(items, windows...)
	items = append(items, coreViews...)
	return js.Render(items...)
}
