// Package ui defines the ExtJS additions injected into Proxmox Backup Server.
package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

// Render returns the generated JavaScript definitions in dependency order.
func Render() []byte {
	items := append(models, mtfModels...)
	items = append(items, scriptSelector, scriptEditWindow, scriptPanel)
	items = append(items, selectors...)
	items = append(items, calendarEventSelector)
	items = append(items, pathSelectors...)
	items = append(items, windows...)
	items = append(items, exclusionPanel)
	items = append(items, mtfChangerGrid)
	items = append(items, mtfDriveGrid)
	items = append(items, notificationBatchPanel)
	items = append(items, tokenPanel)
	items = append(items, mtfJobPanel)
	items = append(items, mtfInventoryPanel)
	items = append(items, mtfMappingPanel)
	items = append(items, restorePanel)
	items = append(items, backupPanel)
	items = append(items, verificationPanel)
	items = append(items, mountPanel)
	items = append(items, targetPanelController)
	items = append(items, targetPanel)
	items = append(items, alertsPanel)
	items = append(items, alertEditWindow)
	items = append(items, notificationTabs)
	items = append(items, restoreModesStore)
	items = append(items, restoreJobEdit)
	items = append(items, backupModeStores)
	items = append(items, backupJobEdit)
	items = append(items, mtfJobEdit)
	items = append(items, notificationBatchEdit)
	items = append(items, coreViews...)
	return js.Render(items...)
}
