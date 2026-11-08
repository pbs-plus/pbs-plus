package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/api/extjs"
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const (
	jobFilterAll     = "all"
	jobFilterOverdue = "overdue"
)

var (
	jobFilterAllOption      = js.Option{Value: jobFilterAll, Text: "All"}
	jobFilterRunningOption  = js.Option{Value: extjs.JobStateRunning, Text: "Running / Queued"}
	jobFilterFailedOption   = js.Option{Value: extjs.JobStateFailed, Text: "Failed"}
	jobFilterWarningOption  = js.Option{Value: extjs.JobStateWarning, Text: "Warnings"}
	jobFilterNeverOption    = js.Option{Value: extjs.JobStateNever, Text: "Never run"}
	jobFilterOverdueOption  = js.Option{Value: jobFilterOverdue, Text: "Overdue / Stale"}
	restoreJobStatusFilters = js.StatusFilterTool(jobFilterAllOption, jobFilterRunningOption, jobFilterFailedOption, jobFilterWarningOption, jobFilterNeverOption)
	backupJobStatusFilters  = js.StatusFilterTool(jobFilterAllOption, jobFilterRunningOption, jobFilterFailedOption, jobFilterWarningOption, jobFilterNeverOption, jobFilterOverdueOption)
)

var recentRunsColumn = js.Column{Text: "Recent Runs", DataIndex: "recent_runs", Width: 150, Sortable: new(false), Renderer: "PBS.PlusUtils.render_recent_runs"}

var recentRunsListeners = js.Obj{"cellclick": js.Raw("PBS.PlusUtils.onRecentRunClick")}
