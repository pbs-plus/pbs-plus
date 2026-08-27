package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var activeMountsModel = js.Model{
	Name:       "pbs-model-active-mounts",
	Fields:     js.Fields("datastore", "namespace", "backup-type", "backup-id", "backup-time", "file-name", "mode", "mount-point", "mounted", "commit-capable"),
	IDProperty: "mount-point",
}

var activeMountsPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.ActiveMountsPanel", XType: "pbsPlusActiveMountsPanel",
	Title: "Active Mounts",
	Store: js.Store{
		StoreID: "pbs-plus-active-mounts", Model: "pbs-model-active-mounts",
		Interval: 5000, APIPath: "/api2/extjs/config/d2d-mounts", Sorters: "mount-point",
	},
	Listeners: js.Listeners{Activate: "startStore", Deactivate: "stopStore", BeforeDestroy: "stopStore"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
		`),
		"startStore": js.Func("", `
			this.getView().getStore().rstore.startUpdate();
		`),
		"stopStore": js.Func("", `
			this.getView().getStore().rstore.stopUpdate();
		`),
		"reload": js.Func("", `
			this.getView().getStore().rstore.load();
		`),
		"commit": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let d = rec.data;
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/d2d-commit/" + encodeURIComponent(encodePathValue(d.datastore)),
				method: "POST",
				params: { "mount-path": d["mount-point"] },
				waitMsgTarget: view,
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
				success: (resp) => {
					Ext.create("PBS.plusWindow.TaskViewer", {
						upid: resp.result.data,
						taskDone: () => view.getStore().rstore.load(),
					}).show();
				},
			});
		`),
		"unmount": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let d = rec.data;
			let me = this;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Unmount '{0}'? Uncommitted changes of read-write mounts are lost."), d["mount-point"]),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-unmount/" + encodeURIComponent(encodePathValue(d.datastore)),
						method: "POST",
						params: { "mount-path": d["mount-point"], force: 1 },
						waitMsgTarget: view,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: (resp) => {
							Ext.create("PBS.plusWindow.TaskViewer", {
								upid: resp.result.data,
								taskDone: () => view.getStore().rstore.load(),
							}).show();
						},
					});
				},
			);
		`),
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload"},
	},
	Columns: []js.Column{
		{Text: "Datastore", DataIndex: "datastore", Width: 120},
		{Text: "Namespace", DataIndex: "namespace", Width: 110, Renderer: js.Func("v", `return v ? Ext.String.htmlEncode(v) : "-";`)},
		{Text: "Group", DataIndex: "backup-id", Flex: 1, Renderer: js.Func("v, meta, rec", `
			return Ext.String.htmlEncode(rec.get("backup-type") + "/" + v);
		`)},
		{Text: "Backup Time", DataIndex: "backup-time", Width: 150, Renderer: js.Func("v", `
			return v ? Ext.Date.format(new Date(v), "Y-m-d H:i:s") : "-";
		`)},
		{Text: "Archive", DataIndex: "file-name", Width: 180, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Mode", DataIndex: "mode", Width: 70, Renderer: js.Func("v", `
			return v === "rw" ? '<i class="fa fa-fw fa-pencil"></i> rw' : '<i class="fa fa-fw fa-lock"></i> ro';
		`)},
		{Text: "Mount Point", DataIndex: "mount-point", Flex: 1.2, Renderer: js.Func("v", `return Ext.String.htmlEncode(v);`)},
		{Text: "Status", DataIndex: "mounted", Width: 90, Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle"></i> ' + gettext("Mounted")
				: '<i class="fa fa-warning"></i> ' + gettext("Offline");
		`)},
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "mount-point", Width: 80, Items: js.Arr{
			js.Obj{
				"handler": "commit",
				"tooltip": js.T("Commit changes to a new snapshot"),
				"getClass": js.Func("v, meta, rec", `
					return rec.get("commit-capable") ? "fa fa-fw fa-upload" : "pmx-hidden";
				`),
				"isActionDisabled": js.Func("v, r, c, i, rec", `
					return !rec.get("commit-capable") || !rec.get("mounted");
				`),
			},
			js.Obj{
				"handler": "unmount",
				"tooltip": js.T("Unmount"),
				"getClass": js.Func("v, meta, rec", `
					return "fa fa-fw fa-eject" + (rec.get("mounted") ? "" : " pmx-opacity-50");
				`),
			},
		}},
	},
}
