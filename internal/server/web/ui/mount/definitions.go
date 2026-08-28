package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func Definitions() []js.Value {
	return []js.Value{activeMountsModel, activeMountsPanel, mountProfilesModel, mountProfilesPanel, composeWindow, mountPanel}
}
