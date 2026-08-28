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
		"initNew": js.Func("view", `
			let me = this;
			Ext.create("Ext.window.Window", {
				title: gettext("Create New Snapshot"),
				modal: true,
				layout: "anchor",
				bodyPadding: 10,
				width: 480,
				items: [{
					xtype: "component",
					cls: "pmx-hint",
					html: gettext("Creates an empty, writable mount for a new snapshot. Add files at the mount path, then use Commit from Active Mounts to save the snapshot. Existing snapshots are unchanged."),
					margin: "0 0 10 0",
				}, {
					xtype: "form",
					anchor: "100%",
					border: false,
					defaults: { anchor: "100%", labelWidth: 120 },
					items: [
					{
						xtype: "combobox",
						name: "datastore",
						fieldLabel: gettext("Datastore"),
						store: "pbs-datastore-list",
						displayField: "store",
						valueField: "store",
						allowBlank: false,
					},
					{
						xtype: "proxmoxtextfield",
						name: "ns",
						fieldLabel: gettext("Namespace"),
						emptyText: gettext("root"),
					},
					{
						xtype: "combobox",
						name: "backup-type",
						fieldLabel: gettext("Backup Type"),
						store: [["host", "host"], ["vm", "vm"], ["ct", "ct"]],
						allowBlank: false,
						value: "host",
					},
					{
						xtype: "proxmoxtextfield",
						name: "backup-id",
						fieldLabel: gettext("Backup ID"),
						allowBlank: false,
					},
					{
						xtype: "proxmoxtextfield",
						name: "mount-path",
						fieldLabel: gettext("Mount Path"),
						emptyText: gettext("Automatic (under /mnt/pbs-plus-restores)"),
					},
					],
				}],
				buttons: [{
					text: gettext("Initialize"),
					handler: function (btn) {
						let win = btn.up("window");
						let form = win.down("form");
						if (!form.isValid()) return;
						let vals = form.getValues();
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-init/" + encodeURIComponent(encodePathValue(vals.datastore)),
							method: "POST",
							params: {
								"ns": vals.ns || "",
								"backup-type": vals["backup-type"],
								"backup-id": vals["backup-id"],
								"mount-path": vals["mount-path"] || "",
							},
							waitMsgTarget: win,
							failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
							success: (resp) => {
								win.close();
								me.reload();
								Ext.create("PBS.plusWindow.TaskViewer", {
									upid: resp.result.data,
									taskDone: () => view.getStore().rstore.load(),
								}).show();
							},
						});
					},
				}],
			}).show();
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
				Ext.String.format(gettext("Unmount '{0}'? Uncommitted changes of read-write mounts are kept and restored on the next read-write mount of this snapshot."), d["mount-point"]),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-unmount/" + encodeURIComponent(encodePathValue(d.datastore)),
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
				},
			);
		`),
		"discard": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let d = rec.data;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Delete the uncommitted changes of '{0}'? This cannot be undone."), d["mount-point"]),
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
		"remount": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let d = rec.data;
			let isInit = !d["backup-time"];
			let params = {
				"ns": d.namespace || "",
				"backup-type": d["backup-type"],
				"backup-id": d["backup-id"],
				"mount-path": d["mount-point"],
			};
			let url = "/api2/extjs/config/d2d-init/" + encodeURIComponent(encodePathValue(d.datastore));
			if (!isInit) {
				params["backup-time"] = d["backup-time"];
				params["file-name"] = d["file-name"];
				params.mode = d.mode;
				url = "/api2/extjs/config/d2d-mount/" + encodeURIComponent(encodePathValue(d.datastore));
			}
			PBS.PlusUtils.API2Request({
				url: url,
				method: "POST",
				params: params,
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
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload"},
		{XType: js.XButton, Text: "New Snapshot", IconCls: "fa fa-plus", Handler: "initNew"},
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
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "mount-point", Width: 140, Items: js.Arr{
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
				"handler": "remount",
				"tooltip": js.T("Remount (restores uncommitted changes of read-write sessions)"),
				"getClass": js.Func("v, meta, rec", `
					return rec.get("mounted") ? "pmx-hidden" : "fa fa-fw fa-play";
				`),
			},
			js.Obj{
				"handler": "unmount",
				"tooltip": js.T("Unmount"),
				"getClass": js.Func("v, meta, rec", `
					return "fa fa-fw fa-eject" + (rec.get("mounted") ? "" : " pmx-opacity-50");
				`),
			},
			js.Obj{
				"handler": "discard",
				"tooltip": js.T("Discard uncommitted changes"),
				"getClass": js.Func("v, meta, rec", `
					return rec.get("mode") === "rw" ? "fa fa-fw fa-trash" : "pmx-hidden";
				`),
			},
		}},
	},
}
