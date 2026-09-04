package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var activeMountsModel = js.Model{
	Name:       "pbs-model-active-mounts",
	Fields:     js.Fields("datastore", "namespace", "backup-type", "backup-id", "backup-time", "file-name", "mode", "backend", "outpost", "profile", "endpoint", "mount-point", "mounted", "commit-capable"),
	IDProperty: "mount-point",
}

var activeMountsPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.ActiveMountsPanel", XType: "pbsPlusActiveMountsPanel",
	Title:             "Active Mounts",
	MultiSelect:       true,
	CheckboxSelection: true,
	Store: js.Store{
		StoreID: "pbs-plus-active-mounts", Model: "pbs-model-active-mounts",
		Interval: 5000, APIPath: "/api2/extjs/config/d2d-mounts", Sorters: "mount-point",
	},
	Listeners: js.Listeners{
		Activate: "startStore", Deactivate: "stopStore", BeforeDestroy: "stopStore",
		SelectionChange: "onSelectionChange",
	},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
		`),
		"onSelectionChange": js.Func("selModel, selected", `
			let view = this.getView();
			let rec = selected[0] || null;
			view.query("proxmoxButton").forEach((btn) => {
				if (btn.enableFn) {
					btn.setDisabled(!btn.enableFn(rec));
				} else if (btn.selModel !== false) {
					btn.setDisabled(!rec);
				} else {
					btn.setDisabled(false);
				}
			});
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
						xtype: "combobox",
						name: "backend",
						fieldLabel: gettext("Backend"),
						store: [["fuse", "FUSE"], ["nfs", "NFSv3"]],
						value: "fuse",
						editable: false,
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
								"backend": vals.backend,
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
		"unmountParams": js.Func("d", `
			let params = {};
			if (d.outpost) {
				params.ns = d.namespace || "";
				params["backup-type"] = d["backup-type"];
				params["backup-id"] = d["backup-id"];
				params["backup-time"] = d["backup-time"];
				params["file-name"] = d["file-name"];
				params.outpost = d.outpost;
				if (d["share-name"]) params["share-name"] = d["share-name"];
			} else {
				params["mount-path"] = d["mount-point"];
			}
			return params;
		`),
		"requestUnmount": js.Func("view, recs, force", `
			for (const rec of recs) {
				let d = rec.data;
				let params = this.unmountParams(d);
				if (force) params.force = 1;
				PBS.PlusUtils.API2Request({
					url: "/api2/extjs/config/d2d-unmount/" + encodeURIComponent(encodePathValue(d.datastore)),
					method: "POST",
					params: params,
					waitMsgTarget: view,
					failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					success: () => view.getStore().rstore.load(),
				});
			}
		`),
		"unmountSelected": js.Func("", `
			let me = this;
			let view = this.getView();
			let sel = view.getSelectionModel().getSelection();
			if (!sel.length) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select at least one mount."));
				return;
			}
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Unmount {0} selected mounts? Uncommitted changes of read-write mounts are kept and restored on the next read-write mount of this snapshot."), sel.length),
				(btn) => {
					if (btn !== "yes") return;
					me.requestUnmount(view, sel, false);
				},
			);
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
						params: me.unmountParams(d),
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
			let me = this;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Delete the uncommitted changes of '{0}'? This cannot be undone."), d["mount-point"]),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-unmount/" + encodeURIComponent(encodePathValue(d.datastore)),
						method: "POST",
						params: Ext.apply(me.unmountParams(d), { force: 1 }),
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
			let m = this.mountParams(rec.data);
			PBS.PlusUtils.API2Request({
				url: m.url,
				method: "POST",
				params: m.params,
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
		"mountParams": js.Func("d", `
			let params = {
				"ns": d.namespace || "",
				"backup-type": d["backup-type"],
				"backup-id": d["backup-id"],
			};
			let url;
			if (d.outpost) {
				params["backup-time"] = d["backup-time"];
				params["file-name"] = d["file-name"];
				params.mode = d.mode;
				params.outpost = d.outpost;
				if (d["share-name"]) params["share-name"] = d["share-name"];
				url = "/api2/extjs/config/d2d-mount/" + encodeURIComponent(encodePathValue(d.datastore));
			} else {
				params["mount-path"] = d["mount-point"];
				url = "/api2/extjs/config/d2d-init/" + encodeURIComponent(encodePathValue(d.datastore));
				if (d["backup-time"]) {
					params["backup-time"] = d["backup-time"];
					params["file-name"] = d["file-name"];
					params.mode = d.mode;
					url = "/api2/extjs/config/d2d-mount/" + encodeURIComponent(encodePathValue(d.datastore));
				}
			}
			return { url, params };
		`),
		"remountSelected": js.Func("", `
			let view = this.getView();
			let sel = view.getSelectionModel().getSelection();
			if (!sel.length) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select at least one mount."));
				return;
			}
			for (const rec of sel) {
				let m = this.mountParams(rec.data);
				PBS.PlusUtils.API2Request({
					url: m.url,
					method: "POST",
					params: m.params,
					waitMsgTarget: view,
					failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					success: () => view.getStore().rstore.load(),
				});
			}
		`),
		"commitSelected": js.Func("", `
			let view = this.getView();
			let sel = view.getSelectionModel().getSelection();
			if (!sel.length) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select at least one mount."));
				return;
			}
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Commit the uncommitted changes of {0} selected mounts into new snapshots?"), sel.length),
				(btn) => {
					if (btn !== "yes") return;
					for (const rec of sel) {
						let d = rec.data;
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-commit/" + encodeURIComponent(encodePathValue(d.datastore)),
							method: "POST",
							params: { "mount-path": d["mount-point"] },
							waitMsgTarget: view,
							failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
							success: () => view.getStore().rstore.load(),
						});
					}
				},
			);
		`),
		"discardSelected": js.Func("", `
			let me = this;
			let view = this.getView();
			let sel = view.getSelectionModel().getSelection();
			if (!sel.length) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select at least one mount."));
				return;
			}
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Delete the uncommitted changes of {0} selected mounts? This cannot be undone."), sel.length),
				(btn) => {
					if (btn !== "yes") return;
					me.requestUnmount(view, sel, true);
				},
			);
		`),
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload", SelModel: new(false)},
		{XType: js.XButton, Text: "New Snapshot", IconCls: "fa fa-plus", Handler: "initNew", SelModel: new(false)}, js.Sep(),
		{
			XType: js.XButton, Text: "Remount Selected", IconCls: "fa fa-play", Handler: "remountSelected",
			Disabled: true, EnableFn: js.SelectionEvery(`!r.data.mounted`),
		},
		{
			XType: js.XButton, Text: "Commit Selected", IconCls: "fa fa-upload", Handler: "commitSelected",
			Disabled: true, EnableFn: js.SelectionEvery(`r.data["commit-capable"] && r.data.mounted`),
		},
		{
			XType: js.XButton, Text: "Unmount Selected", IconCls: "fa fa-eject", Handler: "unmountSelected",
			Disabled: true, EnableFn: js.SelectionEvery(`r.data.mounted`),
		},
		{
			XType: js.XButton, Text: "Discard Selected", IconCls: "fa fa-trash-o", Handler: "discardSelected",
			Disabled: true, EnableFn: js.SelectionEvery(`r.data.mode === "rw"`),
		},
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
		{Text: "Backend", DataIndex: "backend", Width: 75, Renderer: js.Func("v", `return v === "nfs" ? "NFSv3" : "FUSE";`)},
		{Text: "Outpost", DataIndex: "outpost", Width: 110, Renderer: js.Func("v", `
			return v ? Ext.String.htmlEncode(v) : "-";
		`)},
		{Text: "Profile", DataIndex: "profile", Width: 150, Renderer: js.Func("v", `
			return v ? Ext.String.htmlEncode(v) : "-";
		`)},
		{Text: "Mount Point", DataIndex: "mount-point", Flex: 1.2, Renderer: js.Func("v, meta, rec", `
			let endpoint = rec.get("endpoint");
			return Ext.String.htmlEncode(endpoint || v);
		`)},
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
