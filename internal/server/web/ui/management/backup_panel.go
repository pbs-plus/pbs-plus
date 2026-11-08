package management

import (
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/server/web/api/extjs"
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const backupRunURL = "/api2/extjs/d2d/backup?"

var backupGroupFn = js.Func("record", `
	const ns = record.get("ns");
	return ns ? "Namespace: " + ns.split("/")[0] : "Namespace: /";
`)

var lastSuccessRenderer = js.Func("value, meta, record", `
	let out = PBS.Utils.render_optional_timestamp(value);
	let missed = record.get("missed_runs");
	if (missed > 0) {
		let count = missed >= `+strconv.Itoa(extjs.MissedRunLimit)+` ? missed + "+" : missed;
		let tip = Ext.String.format(gettext("Missed {0} scheduled run(s) since the last success"), count);
		meta.tdAttr = 'data-qtip="' + Ext.htmlEncode(tip) + '"';
		out += ' <i class="fa fa-exclamation-triangle warning"></i> ' + count;
	}
	return out;
`)

var backupRowStyles = js.Raw(`
	.pbs-row-warning-old-backup {
		background-color: #ffc107 !important;
	}
	.pbs-row-warning-old-backup .x-grid-cell {
		background-color: #ffc107 !important;
	}

	.pbs-row-warning-overdue,
	.pbs-row-warning-overdue .x-grid-cell {
		background-color: #fff3cd !important;
	}

	@media (prefers-color-scheme: dark) {
		.pbs-row-warning-old-backup,
		.pbs-row-warning-old-backup .x-grid-cell {
			background-color: rgba(255, 193, 7, 0.35) !important;
		}
		.pbs-row-warning-overdue,
		.pbs-row-warning-overdue .x-grid-cell {
			background-color: rgba(255, 193, 7, 0.15) !important;
		}
	}
`)

var (
	showJobHistoryTool = js.Tool{Text: "Show job history", Handler: "showJobHistory", Disabled: true, EnableFn: js.EnableOnSingleSelection}
	restoreFromJobTool = js.Tool{Text: "Restore", IconCls: "fa fa-undo", Handler: "restoreFromJob", Disabled: true, EnableFn: js.EnableOnSingleSelection}
	bulkEditTool       = js.Tool{Text: "Bulk Edit", Handler: "bulkEdit", Disabled: true, EnableFn: js.SelectionEvery(js.JobIdle)}
)

var backupPanel = js.Panel{
	Name: "PBS.config.DiskBackupJobView", XType: "pbsDiskBackupJobView",
	Title: "Disk Backup Jobs", StateID: "grid-disk-backups-v1",
	MultiSelect: true, CheckboxSelection: true,
	Store: js.Store{
		StoreID: "pbs-disk-backup-status", Model: "pbs-disk-backup-status", Interval: 5000,
		Sorters: "id", GroupField: "ns",
	},
	Grouping: &js.Grouping{
		HeaderTemplate: js.GroupHeader("Item"),
		FormatName:     js.Func("ns", `return ns;`),
		GroupProperty:  "ns",
		GroupFn:        backupGroupFn,
	},
	ViewConfig: &js.ViewConfig{GetRowClass: js.Func("record", `
		if (record.get("stale")) {
			return "pbs-row-warning-old-backup";
		}
		return record.get("missed_runs") > 0 ? "pbs-row-warning-overdue" : "";
	`)},
	Listeners:    js.Listeners{ItemDblClick: "editJob", ItemContextMenu: "onItemContextMenu"},
	ListenersRaw: recentRunsListeners,
	Controller: js.Controller{Methods: js.MergeMethods(js.JobFilterMethods("id", "target", "ns", "comment", "subpath"), map[string]js.Raw{
		"onItemContextMenu": js.ContextMenu(
			js.RunIdleJobsTool, js.StopJobsTool, js.Sep(),
			restoreFromJobTool, js.EditIdleJobTool, bulkEditTool, js.DuplicateJobTool, js.Sep(),
			js.ShowLogTool, js.ShowSuccessLogTool, showJobHistoryTool,
		),
		"bulkEdit": js.Func("", `
			const me = this;
			const recs = me.getView().getSelection();
			if (!recs.length) return;
			const stores = Ext.Array.unique(recs.map((r) => r.get("store")));
			Ext.create("PBS.window.D2DBulkJobEdit", {
				url: "/api2/extjs/config/disk-backup-bulk",
				jobIds: recs.map((r) => r.getId()),
				commonStore: stores.length === 1 ? stores[0] : null,
				fields: [
					{ name: "schedule", label: gettext("Schedule"), field: { xtype: "pbsD2DCalendarEvent", emptyText: gettext("none (disabled)") } },
					{ name: "store", label: gettext("Local Datastore"), field: { xtype: "`+string(js.XDataStoreSelector)+`" } },
					{ name: "ns", label: gettext("Namespace"), field: { xtype: "pbsD2DNamespaceSelector", emptyText: gettext("Root") } },
					{ name: "retry", label: gettext("Number of retries"), field: { xtype: "proxmoxintegerfield", minValue: 0, emptyText: "0" } },
					{ name: "retry-interval", label: gettext("Retry interval (minutes)"), field: { xtype: "proxmoxintegerfield", minValue: 1, emptyText: "1" } },
					{ name: "pre_script", label: gettext("Pre-Backup Script"), field: { xtype: "pbsD2DScriptSelector" } },
					{ name: "post_script", label: gettext("Post-Backup Script"), field: { xtype: "pbsD2DScriptSelector" } },
					{ name: "comment", label: gettext("Comment"), field: { xtype: "proxmoxtextfield" } },
				],
				listeners: { destroy: () => me.reload() },
			}).show();
		`),
		"restoreFromJob": js.Func("", `
			const selection = this.getView().getSelection();
			if (selection.length !== 1) return;
			const job = selection[0];
			Ext.create("PBS.D2DManagement.RestoreJobEdit", {
				autoShow: true,
				snapshotGroup: job.getId(),
				jobData: { store: job.get("store"), ns: job.get("ns"), "dest-target": job.get("target"), remove_on_success: true },
			});
		`),
		"addJob":             js.OpenEditWindow("PBS.D2DManagement.BackupJobEdit", ""),
		"editJob":            js.EditJobWindow("PBS.D2DManagement.BackupJobEdit"),
		"duplicateJob":       js.DuplicateJobWindow("PBS.D2DManagement.BackupJobEdit"),
		"removeJobs":         js.ConfirmRemove("/api2/extjs/config/disk-backup/", "encodePathValue(rec.getId())", "Remove selected entries?"),
		"openTaskLog":        js.OpenTaskLog("last-run-upid"),
		"openSuccessTaskLog": js.OpenTaskLog("last-successful-upid"),
		"runJobs":            js.RunJobs(backupRunURL, "Start backup job '{0}'?", "Start backup jobs '{0}'?"),
		"exportCSV":          js.Func("", `window.open(pbsPlusBaseUrl + "/api2/extjs/d2d/backup/export", "_blank");`),
		"stopJobs":           js.StopJobs(backupRunURL, "Stop backup job '{0}'?", "Stop backup jobs '{0}'?"),
		"showJobHistory": js.Func("", `
			const me = this;
			const view = me.getView();
			const selection = view.getSelection();
			if (selection.length !== 1) return;
			const jobId = selection[0].getId();
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/disk-backup/" + encodeURIComponent(encodePathValue(jobId)) + "/upids",
				method: "GET",
				waitMsgTarget: view,
				success: function (response) {
					const upids = response.result.data || [];
					if (!upids.length) {
						Ext.Msg.alert(gettext("Info"), gettext("No task logs found for this job."));
						return;
					}
					const upidStore = Ext.create("Ext.data.Store", {
						fields: ["upid", "starttime", "endtime", "status", "status_parsed", "duration"],
						data: upids,
					});
					Ext.create("Ext.window.Window", {
						title: gettext("Task Logs for Job: ") + Ext.String.htmlEncode(jobId),
						width: 900,
						height: 400,
						modal: true,
						layout: "fit",
						items: [{
							xtype: "grid",
							store: upidStore,
							columns: [
								{ text: gettext("Start Time"), dataIndex: "starttime", flex: 1, renderer: (v) => Proxmox.Utils.render_timestamp(v) },
								{ text: gettext("End Time"), dataIndex: "endtime", flex: 1, renderer: (v) => v ? Proxmox.Utils.render_timestamp(v) : "-" },
								{ text: gettext("Duration"), dataIndex: "duration", flex: 1, renderer: (v) => v === undefined || v === null ? "-" : Proxmox.Utils.format_duration_long(v) },
								{ text: gettext("Status"), dataIndex: "status", flex: 2, renderer: PBS.PlusUtils.render_task_status },
							],
							listeners: {
								itemdblclick: (grid, record) => Ext.create("PBS.plusWindow.TaskViewer", { upid: record.get("upid") }).show(),
							},
						}],
						buttons: [{ text: gettext("Close"), handler: function () { this.up("window").close(); } }],
					}).show();
				},
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
			});
		`),
		"init": js.Func("view", `
			PBS.PlusUtils.monStoreErrors(view, view.getStore().rstore);
			this.initJobFilters(view);
			if (!document.getElementById("pbs-backup-job-styles")) {
				const style = document.createElement("style");
				style.id = "pbs-backup-job-styles";
				style.innerHTML = `+"`"+string(backupRowStyles)+"`"+`;
				document.head.appendChild(style);
			}
			view.getStore().setGrouper({ property: "ns", groupFn: `+string(backupGroupFn)+` });
		`),
	})},
	Tbar: []js.Tool{
		js.AddJobTool, js.DuplicateJobTool, js.EditIdleJobTool, bulkEditTool, js.RemoveIdleJobsTool, js.Sep(),
		js.ShowLogTool, js.ShowSuccessLogTool, showJobHistoryTool, js.Sep(),
		js.RunIdleJobsTool, js.StopJobsTool, js.Sep(),
		restoreFromJobTool,
		{Text: "Export CSV", Handler: "exportCSV", SelModel: new(false)},
		js.Fill(), backupJobStatusFilters, js.SearchTool(),
	},
	Columns: []js.Column{
		{Text: "Job ID", DataIndex: "id", Flex: 1, MaxWidth: 220, MinWidth: 75, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
		{Text: "Target", DataIndex: "target", Width: 120, Sortable: new(true)},
		{Text: "Subpath", DataIndex: "subpath", Width: 120, Sortable: new(true)},
		{Text: "Datastore", DataIndex: "store", Width: 120, Sortable: new(true), Hidden: true},
		{Text: "Namespace", DataIndex: "ns", Width: 120, Sortable: new(true)},
		{Text: "Schedule", DataIndex: "schedule", Flex: 1, MaxWidth: 220, MinWidth: 80, Sortable: new(true)},
		{Text: "Legacy Xattr", DataIndex: "legacy-xattr", Width: 120, Sortable: new(true)},
		{Text: "Last Success", DataIndex: "last-successful-endtime", Width: 180, Sortable: new(true), Renderer: lastSuccessRenderer},
		{Text: "Last Attempt", DataIndex: "last-run-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Duration", DataIndex: "duration", Width: 60, Renderer: "Proxmox.Utils.render_duration"},
		{Text: "Read Speed", DataIndex: "read_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Read Total", DataIndex: "read_total_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Target Size", DataIndex: "target_size_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Processing Speed", DataIndex: "processing_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Files Processed", DataIndex: "current_file_count", Width: 60, Hidden: true, Renderer: js.CountIfSet},
		{Text: "Folders Processed", DataIndex: "current_folder_count", Width: 60, Hidden: true, Renderer: js.CountIfSet},
		recentRunsColumn,
		{Text: "Status", DataIndex: "last-run-state", Flex: 1, Renderer: "PBS.PlusUtils.render_task_status"},
		{Text: "Next Run", DataIndex: "next-run", Width: 150, Sortable: new(true), Hidden: true, Renderer: "PBS.Utils.render_next_task_run"},
		{Text: "Comment", DataIndex: "comment", Flex: 2, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
	},
}
