package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var recentRunsColumn = js.Column{Text: "Recent Runs", DataIndex: "recent_runs", Width: 150, Sortable: new(false), Renderer: "PBS.PlusUtils.render_recent_runs"}

var recentRunsListeners = js.Obj{"cellclick": js.Raw("PBS.PlusUtils.onRecentRunClick")}
