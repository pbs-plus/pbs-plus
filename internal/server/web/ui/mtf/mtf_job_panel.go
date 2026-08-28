package mtf

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

const mtfJobURL = "/api2/extjs/d2d/mtf-job?job="

var mtfJobPanel = js.Panel{
	Name: "PBS.MtfManagement.JobView", XType: "pbsMtfJobView",
	Title: "MTF Migration Jobs", StateID: "grid-mtf-jobs-v1",
	Store:      js.Store{StoreID: "pbs-mtf-job", Model: "pbs-mtf-job", Interval: 5000, Sorters: "id"},
	ViewConfig: &js.ViewConfig{TrackOver: new(false)},
	Listeners:  js.Listeners{ItemDblClick: "editJob"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"addJob":  js.OpenEditWindow("PBS.MtfManagement.JobEdit", ""),
		"editJob": js.EditSelection("PBS.MtfManagement.JobEdit", "jobId", "id", "autoShow"),
		"removeJobs": js.ConfirmRemove(
			"/api2/extjs/config/mtf-job/",
			"encodePathValue(rec.getId())",
			"Delete MTF job(s) '{0}'?",
		),
		"runJob": js.Func("", `
			let me = this;
			let view = me.getView();
			let selection = view.getSelection();
			if (!selection || selection.length < 1) {
				return;
			}
			PBS.PlusUtils.API2Request({
				url: "`+mtfJobURL+`" + encodeURIComponent(encodePathValue(selection[0].data.id)),
				method: "POST",
				waitMsgTarget: view,
				success: function (response) {
					let upid = response.result.data;
					me.reload();
					if (upid) {
						Ext.create("PBS.plusWindow.TaskViewer", { upid: upid, taskDone: () => me.reload() }).show();
					}
				},
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
			});
		`),
		"stopJob": js.Func("", `
			let me = this;
			let view = me.getView();
			let selection = view.getSelection();
			if (!selection || selection.length < 1) {
				return;
			}
			let id = selection[0].data.id;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Stop migration job '{0}'?"), id),
				function (btn) {
					if (btn !== "yes") {
						return;
					}
					PBS.PlusUtils.API2Request({
						url: "`+mtfJobURL+`" + encodeURIComponent(encodePathValue(id)),
						method: "DELETE",
						waitMsgTarget: view,
						success: () => me.reload(),
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					});
				},
			);
		`),
		"openTaskLog": js.OpenTaskLog("last-run-upid"),
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
			view.getStore().on("datachanged", function () {
				let sel = view.getSelectionModel().getSelection();
				view.query("proxmoxButton").forEach(function (btn) {
					if (btn.enableFn && btn.selModel) {
						let rec = sel.length > 0 ? sel[0] : null;
						btn.setDisabled(!rec || btn.enableFn(rec) === false);
					}
				});
			});
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Add", Handler: "addJob", SelModel: new(false)},
		{Text: "Edit", Handler: "editJob", Disabled: true, EnableFn: js.EnableOnRecord},
		{Text: "Remove", Handler: "removeJobs", Disabled: true, EnableFn: js.EnableOnRecord}, js.Sep(),
		{Text: "Run now", Handler: "runJob", Disabled: true, EnableFn: js.EnableOnRecord},
		{Text: "Stop", Handler: "stopJob", Disabled: true, EnableFn: js.Func("rec", `
			if (!rec) return false;
			return rec.data["last-run-upid"] && !rec.data["last-run-state"];
		`)}, js.Sep(),
		{Text: "Show Log", Handler: "openTaskLog", Disabled: true, EnableFn: js.Func("rec", `return !!rec.data["last-run-upid"];`)},
	},
	Columns: []js.Column{
		{Text: "Job ID", DataIndex: "id", Flex: 1, Sortable: new(true)},
		{Text: "Source", DataIndex: "source_label", Flex: 1.4, Sortable: new(true), Renderer: js.Func("v, meta, rec", `
			const kind = rec.get("source_kind") || "";
			return '<i class="fa fa-archive"></i> ' + (v || rec.get("source_ref")) + ' <span style="color:#888">(' + kind + ')</span>';
		`)},
		{Text: "Datastore", DataIndex: "datastore", Flex: 1, Sortable: new(true)},
		{Text: "Namespace", DataIndex: "namespace", Flex: 1, Sortable: new(true), Renderer: js.Func("v", `return v || "<span style='color:#888'>/</span>";`)},
		{Text: "Status", DataIndex: "last-run-status", Width: 100, Renderer: "PBS.PlusUtils.render_task_status"},
		{Text: "Last Run", DataIndex: "last-run-endtime", Width: 150, Renderer: js.Func("value", `
			if (!value) return "-";
			return Ext.Date.format(new Date(value * 1000), "Y-m-d H:i:s");
		`)},
		{Text: "Duration", DataIndex: "duration", Width: 60, Renderer: "Proxmox.Utils.render_duration"},
		{Text: "Read Speed", DataIndex: "read_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Read Total", DataIndex: "read_total_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Processing Speed", DataIndex: "processing_speed_human", Width: 60, Renderer: js.DashIfEmpty},
		{Text: "Files Processed", DataIndex: "current_file_count", Width: 60, Hidden: true, Renderer: js.Func("value", `
			if (!value && value !== 0) return "-";
			return value.toLocaleString();
		`)},
	},
}
