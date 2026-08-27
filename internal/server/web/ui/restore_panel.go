package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

const restoreRunURL = "/api2/extjs/d2d/restore?"

var restorePanel = js.Panel{
	Name: "PBS.config.DiskRestoreJobView", XType: "pbsDiskRestoreJobView",
	Title: "Disk Restore Jobs", StateID: "grid-disk-restore-jobs-v1",
	MultiSelect: true, CheckboxSelection: true,
	Store: js.Store{
		StoreID: "pbs-disk-restore-job-status", Model: "pbs-disk-restore-job-status", Interval: 5000,
		Sorters: "id", GroupField: "dest-target",
	},
	Grouping: &js.Grouping{
		HeaderTemplate: `{name:this.formatName} ({rows.length} Item{[values.rows.length > 1 ? "s" : ""]})`,
		FormatName:     js.Func("target", `return target;`),
		GroupProperty:  "dest-target",
		GroupFn:        restoreGroupFn,
	},
	Listeners: js.Listeners{ItemDblClick: "editJob"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onSearchKeyUp":      searchFilter("id", "ns", "snapshot", "src-path", "dest-target", "comment", "dest-subpath"),
		"addJob":             openEditWindow("PBS.D2DManagement.RestoreJobEdit", ""),
		"editJob":            editJobWindow("PBS.D2DManagement.RestoreJobEdit"),
		"duplicateJob":       duplicateJobWindow("PBS.D2DManagement.RestoreJobEdit"),
		"removeJobs":         confirmRemove("/api2/extjs/config/disk-restore/", "encodePathValue(rec.getId())", "Remove selected entries?"),
		"openTaskLog":        openTaskLog("last-run-upid"),
		"openSuccessTaskLog": openTaskLog("last-successful-upid"),
		"runJobs":            runJobs(restoreRunURL, "Start restore job '{0}'?", "Start restore jobs '{0}'?"),
		"stopJobs": js.Func("", `
			const me = this;
			const view = me.getView();
			const recs = view.getSelection();
			if (!recs.length) return;
			const jobs = recs
				.map((r) => r.data["last-run-upid"] ? { id: r.getId(), upid: r.data["last-run-upid"] } : null)
				.filter(Boolean);
			if (!jobs.length) return;
			const list = jobs.map((j) => Ext.String.htmlEncode(j.id)).join("', '");
			const msg = jobs.length > 1
				? Ext.String.format(gettext("Stop restore jobs '{0}'?"), list)
				: Ext.String.format(gettext("Stop restore job '{0}'?"), list);
			Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
				if (btn !== "yes") return;
				PBS.PlusUtils.API2Request({
					url: "`+restoreRunURL+`" + jobs.map((j) => "job=" + encodeURIComponent(encodePathValue(j.id))).join("&"),
					method: "DELETE",
					waitMsgTarget: view,
					// The task viewer drives the reload once each PBS task ends.
					success: () => {},
					failure: () => {},
				});
			});
		`),
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
			view.getStore().setGrouper({ property: "dest-target", groupFn: `+string(restoreGroupFn)+` });
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Add Job", Handler: "addJob", SelModel: new(false)},
		{Text: "Duplicate Job", Handler: "duplicateJob", Disabled: true, EnableFn: enableOnSingleSelection},
		{Text: "Edit Job", Handler: "editJob", Disabled: true, EnableFn: selectionOne(`!recs[0].data["last-run-upid"] || !!recs[0].data["last-run-state"]`)},
		{Text: "Remove Job(s)", Handler: "removeJobs", Disabled: true, EnableFn: selectionEvery(jobIdle)}, js.Sep(),
		{Text: "Show Log", Handler: "openTaskLog", Disabled: true, EnableFn: selectionOne(`!!recs[0].data["last-run-upid"]`)}, js.Sep(),
		{Text: "Run Job(s)", Handler: "runJobs", Disabled: true, EnableFn: selectionEvery(jobIdle)},
		{Text: "Stop Job(s)", Handler: "stopJobs", Disabled: true, EnableFn: selectionEvery(jobStoppable)},
		js.Fill(), searchTool(),
	},
	Columns: []js.Column{
		{Text: "Job ID", DataIndex: "id", Flex: 1, MaxWidth: 220, MinWidth: 75, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
		{Text: "Snapshot", DataIndex: "snapshot_human", Width: 120, Sortable: new(true)},
		{Text: "Target Destination", DataIndex: "dest-target", Width: 120, Sortable: new(true)},
		{Text: "Namespace", DataIndex: "ns", Width: 120, Sortable: new(true)},
		{Text: "Datastore", DataIndex: "store", Width: 120, Sortable: new(true), Hidden: true},
		{Text: "Last Attempt", DataIndex: "last-run-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Duration", DataIndex: "duration", Width: 60, Renderer: "Proxmox.Utils.render_duration"},
		{Text: "Read Speed", DataIndex: "read_speed_human", Width: 60, Renderer: dashIfEmpty},
		{Text: "Read Total", DataIndex: "read_total_human", Width: 60, Renderer: dashIfEmpty},
		{Text: "Target Size", DataIndex: "target_size_human", Width: 60, Renderer: dashIfEmpty},
		{Text: "Processing Speed", DataIndex: "processing_speed_human", Width: 60, Renderer: dashIfEmpty},
		{Text: "Files Processed", DataIndex: "current_file_count", Width: 60, Hidden: true, Renderer: countIfSet},
		{Text: "Folders Processed", DataIndex: "current_folder_count", Width: 60, Hidden: true, Renderer: countIfSet},
		{Text: "Status", DataIndex: "last-run-state", Flex: 1, Renderer: "PBS.PlusUtils.render_task_status"},
		{Text: "Comment", DataIndex: "comment", Flex: 2, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
	},
}

var restoreGroupFn = js.Func("record", `
	const target = record.get("dest-target");
	return target ? "Target: " + target : "Target: N/A";
`)
