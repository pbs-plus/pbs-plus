package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const backupRunURL = "/api2/extjs/d2d/backup?"

var backupGroupFn = js.Func("record", `
	const ns = record.get("ns");
	return ns ? "Namespace: " + ns.split("/")[0] : "Namespace: /";
`)

var backupRowStyles = js.Raw(`
	.pbs-row-warning-old-backup {
		background-color: #ffc107 !important;
	}
	.pbs-row-warning-old-backup .x-grid-cell {
		background-color: #ffc107 !important;
	}

	@media (prefers-color-scheme: dark) {
		.pbs-row-warning-old-backup,
		.pbs-row-warning-old-backup .x-grid-cell {
			background-color: rgba(255, 193, 7, 0.35) !important;
		}
	}
`)

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
		return record.get("stale") ? "pbs-row-warning-old-backup" : "";
	`)},
	Listeners: js.Listeners{ItemDblClick: "editJob"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onSearchKeyUp":      js.SearchFilter("id", "target", "ns", "comment", "subpath"),
		"addJob":             js.OpenEditWindow("PBS.D2DManagement.BackupJobEdit", ""),
		"editJob":            js.EditJobWindow("PBS.D2DManagement.BackupJobEdit"),
		"duplicateJob":       js.DuplicateJobWindow("PBS.D2DManagement.BackupJobEdit"),
		"removeJobs":         js.ConfirmRemove("/api2/extjs/config/disk-backup/", "encodePathValue(rec.getId())", "Remove selected entries?"),
		"openTaskLog":        js.OpenTaskLog("last-run-upid"),
		"openSuccessTaskLog": js.OpenTaskLog("last-successful-upid"),
		"runJobs":            js.RunJobs(backupRunURL, "Start backup job '{0}'?", "Start backup jobs '{0}'?"),
		"exportCSV":          js.Func("", `window.open(pbsPlusBaseUrl + "/api2/extjs/d2d/backup/export", "_blank");`),
		"stopJobs": js.Func("", `
			const me = this;
			const view = me.getView();
			const recs = view.getSelection();
			if (!recs.length) return;
			const jobs = recs
				.map((r) => {
					const upid = r.data["last-run-upid"] || "";
					const hasPlus = (r.data["last-run-state"] || "").startsWith("QUEUED:");
					return hasPlus || upid ? { id: r.getId(), upid, hasPlus, hasPBSTask: !!upid } : null;
				})
				.filter(Boolean);
			if (!jobs.length) return;
			const list = jobs.map((j) => Ext.String.htmlEncode(j.id)).join("', '");
			const msg = jobs.length > 1
				? Ext.String.format(gettext("Stop backup jobs '{0}'?"), list)
				: Ext.String.format(gettext("Stop backup job '{0}'?"), list);
			Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
				if (btn !== "yes") return;
				const plusJobs = jobs.filter((j) => j.hasPlus);
				if (plusJobs.length > 0) {
					PBS.PlusUtils.API2Request({
						url: "`+backupRunURL+`" + plusJobs.map((j) => "job=" + encodeURIComponent(encodePathValue(j.id))).join("&"),
						method: "DELETE",
						waitMsgTarget: view,
						success: () => {
							if (!jobs.some((j) => j.hasPBSTask)) {
								me.reload();
							}
						},
						// The per-task deletes below still run.
						failure: () => {},
					});
				}
				jobs.filter((j) => j.hasPBSTask).forEach((job) => {
					const task = Proxmox.Utils.parse_task_upid(job.upid);
					Proxmox.Utils.API2Request({
						url: "/api2/extjs/nodes/" + task.node + "/tasks/" + encodeURIComponent(job.upid),
						method: "DELETE",
						waitMsgTarget: view,
						success: () => me.reload(),
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					});
				});
			});
		`),
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
						fields: ["upid", "starttime", "endtime", "status", "duration"],
						data: upids.map((item) => {
							const task = Proxmox.Utils.parse_task_upid(item.upid);
							return {
								upid: item.upid,
								starttime: task.starttime,
								endtime: item.endtime,
								status: item.status,
								duration: item.endtime && task.starttime ? item.endtime - task.starttime : null,
							};
						}),
						sorters: [{ property: "starttime", direction: "DESC" }],
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
								{ text: gettext("Duration"), dataIndex: "duration", flex: 1, renderer: (v) => v !== null ? Proxmox.Utils.format_duration_long(v) : "-" },
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
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
			if (!document.getElementById("pbs-backup-job-styles")) {
				const style = document.createElement("style");
				style.id = "pbs-backup-job-styles";
				style.innerHTML = `+"`"+string(backupRowStyles)+"`"+`;
				document.head.appendChild(style);
			}
			view.getStore().setGrouper({ property: "ns", groupFn: `+string(backupGroupFn)+` });
		`),
	}},
	Tbar: []js.Tool{
		js.AddJobTool, js.DuplicateJobTool, js.EditIdleJobTool, js.RemoveIdleJobsTool, js.Sep(),
		js.ShowLogTool, js.ShowSuccessLogTool,
		{Text: "Show job history", Handler: "showJobHistory", Disabled: true, EnableFn: js.EnableOnSingleSelection}, js.Sep(),
		js.RunIdleJobsTool, js.StopJobsTool, js.Sep(),
		{Text: "Export CSV", Handler: "exportCSV", SelModel: new(false)},
		js.Fill(), js.SearchTool(),
	},
	Columns: []js.Column{
		{Text: "Job ID", DataIndex: "id", Flex: 1, MaxWidth: 220, MinWidth: 75, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
		{Text: "Target", DataIndex: "target", Width: 120, Sortable: new(true)},
		{Text: "Subpath", DataIndex: "subpath", Width: 120, Sortable: new(true)},
		{Text: "Datastore", DataIndex: "store", Width: 120, Sortable: new(true), Hidden: true},
		{Text: "Namespace", DataIndex: "ns", Width: 120, Sortable: new(true)},
		{Text: "Schedule", DataIndex: "schedule", Flex: 1, MaxWidth: 220, MinWidth: 80, Sortable: new(true)},
		{Text: "Legacy Xattr", DataIndex: "legacy-xattr", Width: 120, Sortable: new(true)},
		{Text: "Last Success", DataIndex: "last-successful-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Last Attempt", DataIndex: "last-run-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Duration", DataIndex: "duration", Width: 60, Renderer: "Proxmox.Utils.render_duration"},
		{Text: "Read Speed", DataIndex: "read_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Read Total", DataIndex: "read_total_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Target Size", DataIndex: "target_size_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Processing Speed", DataIndex: "processing_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Files Processed", DataIndex: "current_file_count", Width: 60, Hidden: true, Renderer: js.CountIfSet},
		{Text: "Folders Processed", DataIndex: "current_folder_count", Width: 60, Hidden: true, Renderer: js.CountIfSet},
		{Text: "Status", DataIndex: "last-run-state", Flex: 1, Renderer: "PBS.PlusUtils.render_task_status"},
		{Text: "Next Run", DataIndex: "next-run", Width: 150, Sortable: new(true), Hidden: true, Renderer: "PBS.Utils.render_next_task_run"},
		{Text: "Comment", DataIndex: "comment", Flex: 2, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
	},
}
