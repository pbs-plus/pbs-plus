package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var notificationBatchPanel = js.Panel{
	Name: "PBS.D2DManagement.NotificationBatchView", XType: "pbsNotificationBatchView",
	Title: "Notification Batches", StateID: "grid-notification-batches-v1",
	MultiSelect: true, CheckboxSelection: true,
	Store: js.Store{
		Fields:  []string{"name", "comment", "notification-mode", "wait-timeout-secs", "send-on-timeout", "created-at", "job-count"},
		APIPath: "/api2/json/d2d/notification-batch", Sorters: "name",
	},
	Listeners: js.Listeners{ItemDblClick: "editBatch"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"addBatch":  js.Func("", `let me = this; Ext.create("PBS.D2DManagement.NotificationBatchEdit", { autoShow: true, listeners: { destroy: () => me.reload() } }).show();`),
		"editBatch": js.Func("", `let me = this; let selection = me.getView().getSelection(); if (!selection || selection.length !== 1) return; Ext.create("PBS.D2DManagement.NotificationBatchEdit", { batchName: selection[0].data.name, autoShow: true, listeners: { destroy: () => me.reload() } }).show();`),
		"removeBatches": js.Func("", `
			let me = this;
			let view = me.getView();
			let recs = view.getSelection();
			if (!recs.length) return;
			Ext.Msg.confirm(gettext("Confirm"), gettext("Remove selected notification batches?"), function (btn) {
				if (btn !== "yes") return;
				recs.forEach(function (rec) {
					PBS.PlusUtils.API2Request({
						url: "/api2/json/d2d/notification-batch?batch=" + encodeURIComponent(rec.data.name),
						method: "DELETE",
						waitMsgTarget: view,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: () => me.reload(),
					});
				});
			});
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Add Batch", Handler: "addBatch", SelModel: new(false)},
		{Text: "Edit Batch", Handler: "editBatch", Disabled: true, EnableFn: enableOnSingleSelection},
		{Text: "Remove", Handler: "removeBatches", Disabled: true, EnableFn: enableOnSelection},
	},
	Columns: []js.Column{
		{Text: "Name", DataIndex: "name", Flex: 1, Sortable: new(true), Renderer: "Ext.String.htmlEncode"},
		{Text: "Comment", DataIndex: "comment", Flex: 2, Sortable: new(true), Renderer: "Ext.String.htmlEncode"},
		{Text: "Notification Mode", DataIndex: "notification-mode", Width: 160, Sortable: new(true), Renderer: js.Func("v", `return v === "legacy-sendmail" ? gettext("Legacy Sendmail") : gettext("Notification System");`)},
		{Text: "Timeout (s)", DataIndex: "wait-timeout-secs", Width: 110, Sortable: new(true)},
		{Text: "Send on Timeout", DataIndex: "send-on-timeout", Width: 130, Sortable: new(true), Renderer: "Proxmox.Utils.format_boolean"},
		{Text: "Jobs", DataIndex: "job-count", Width: 70, Sortable: new(true)},
	},
}
