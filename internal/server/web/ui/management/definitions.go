package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func Definitions() []js.Value {
	items := append(models, scriptSelector, scriptEditWindow, scriptPanel)
	items = append(items, selectors...)
	items = append(items, calendarEventSelector)
	items = append(items, pathSelectors...)
	items = append(items, windows...)
	items = append(items, exclusionPanel, notificationBatchPanel, tokenPanel)
	items = append(items, restorePanel, backupPanel)
	items = append(items, targetPanelController, targetPanel)
	items = append(items, alertsPanel, alertEditWindow)
	items = append(items, notificationTabs, restoreModesStore, restoreJobEdit)
	items = append(items, backupModeStores, backupJobEdit, notificationBatchEdit)
	items = append(items, pathBrowserWindow)
	return items
}
