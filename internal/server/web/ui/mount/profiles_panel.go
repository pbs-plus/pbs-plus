package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var mountProfilesModel = js.Model{
	Name:       "pbs-model-mount-profiles",
	Fields:     js.Fields("id", "datastore", "namespace", "mode", "outpost", "share-name", "mount-path", "schedule", "auto-mount", "replace"),
	IDProperty: "id",
}

var mountProfilesPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.ProfilesPanel", XType: "pbsPlusMountProfilesPanel",
	Title: "Mount Profiles",
	Store: js.Store{StoreID: "pbs-plus-mount-profiles", Model: "pbs-model-mount-profiles", Interval: 5000, APIPath: "/api2/extjs/config/d2d-mount-profiles", Sorters: "id"},
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
		"openEdit": js.Func("view, rec", `
			let isEdit = !!rec;
			let values = isEdit ? rec.data : {};
			let win = Ext.create("Ext.window.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit Batch '{0}'"), values.namespace || gettext("root")) : gettext("Add Mount Batch"),
				width: 480,
				modal: true,
				bodyPadding: 10,
				items: [{
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
						value: values.datastore,
						listeners: {
							change: (cb, v) => {
								let win = cb.up("window");
								let nsCombo = win.down("combobox[name=ns]");
								if (nsCombo) {
									nsCombo.getStore().getProxy().setUrl("/api2/extjs/admin/datastore/" + encodeURIComponent(encodePathValue(v)) + "/namespace");
									nsCombo.getStore().load();
									nsCombo.clearValue();
								}
							},
						},
					},
					{
						xtype: "combobox",
						name: "ns",
						fieldLabel: gettext("Parent Namespace"),
						store: {
							fields: ["ns"],
							autoLoad: !!values.datastore,
							proxy: { type: "proxmox", url: "/api2/extjs/admin/datastore/" + encodeURIComponent(encodePathValue(values.datastore || "")) + "/namespace" },
						},
						displayField: "ns",
						valueField: "ns",
						queryMode: "remote",
						minChars: 0,
						editable: false,
						allowBlank: true,
						emptyText: gettext("root (all namespaces)"),
						value: values.namespace,
						displayTpl: ['<tpl for=".">', '{[values.ns === "" ? "(root — all namespaces)" : Ext.String.htmlEncode(values.ns)]}', '</tpl>'],
						listConfig: {
							itemTpl: ['<div class="x-combo-list-item">{[values.ns === "" ? "(root — all namespaces)" : Ext.String.htmlEncode(values.ns)]}</div>'],
						},
					},
					{
						xtype: "radiogroup",
						fieldLabel: gettext("Mode"),
						items: [
							{ boxLabel: gettext("Read-only"), name: "mode", inputValue: "ro", checked: !isEdit || values.mode === "ro" },
							{ boxLabel: gettext("Read-write (commit-capable)"), name: "mode", inputValue: "rw", checked: values.mode === "rw" },
						],
					},
					{
						xtype: "combobox",
						name: "outpost",
						fieldLabel: gettext("Outpost"),
						store: {
							fields: ["name", "type", "running"],
							autoLoad: true,
							proxy: { type: "pbsplus", url: pbsPlusBaseUrl + "/api2/extjs/config/d2d-outposts" },
						},
						displayField: "name",
						valueField: "name",
						queryMode: "local",
						editable: false,
						emptyText: gettext("None (mount locally)"),
						value: values.outpost,
						listeners: {
							change: (cb, v) => {
								let win = cb.up("window");
								win.down("textfield[name=mount-path]").setDisabled(!!v);
								win.down("textfield[name=share-name]").setDisabled(!v);
							},
						},
					},
					{
						xtype: "proxmoxtextfield",
						name: "share-name",
						fieldLabel: gettext("Share Name"),
						emptyText: gettext("Outpost name"),
						value: values["share-name"],
						disabled: !values.outpost,
					},
					{
						xtype: "proxmoxtextfield",
						name: "mount-path",
						fieldLabel: gettext("Local Root Path"),
						emptyText: gettext("Automatic (under /mnt/pbs-plus-restores)"),
						value: values["mount-path"],
						disabled: !!values.outpost,
					},
					{
						xtype: "pbsD2DCalendarEvent",
						name: "schedule",
						fieldLabel: gettext("Check Schedule"),
						value: values.schedule || undefined,
						editable: true,
					},
					{
						xtype: "proxmoxcheckbox",
						name: "auto-mount",
						fieldLabel: gettext("Auto-mount at startup"),
						inputValue: "1",
						uncheckedValue: "0",
						checked: !!values["auto-mount"],
					},
					{
						xtype: "proxmoxcheckbox",
						name: "replace",
						fieldLabel: gettext("Replace on new snapshot"),
						inputValue: "1",
						uncheckedValue: "0",
						checked: !isEdit || !!values.replace,
					},
						{
							xtype: "displayfield",
							value: gettext("Mounts the newest snapshot of every group under the parent namespace; each namespace appears as its own directory inside the share or root path."),
						},
					],
				}],
				buttons: [
					{
						text: isEdit ? gettext("Save") : gettext("Create"),
						handler: (btn) => {
							let me = this;
							let w = btn.up("window");
							let form = w.down("form");
							if (!form.isValid()) return;
							let vals = form.getValues();
							let params = {
								datastore: vals.datastore,
								ns: vals.ns || "",
								mode: vals.mode,
								outpost: vals.outpost || "",
								"share-name": vals["share-name"] || "",
								"mount-path": vals["mount-path"] || "",
								"schedule": vals.schedule || "",
								"auto-mount": vals["auto-mount"] === "1" ? 1 : 0,
								"replace": vals["replace"] === "1" ? 1 : 0,
							};
							let url = "/api2/extjs/config/d2d-mount-profiles";
							let method = "POST";
							if (isEdit) {
								method = "PUT";
								url += "/" + encodeURIComponent(encodePathValue(rec.data.id));
							}
							PBS.PlusUtils.API2Request({
								url,
								method,
								params,
								waitMsgTarget: w,
								failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
								success: () => {
									w.close();
									const panel = me.getView();
									if (panel) panel.getStore().rstore.load();
								},
							});
						},
					},
				],
			});
			win.show();
		`),
		"add": js.Func("view", `
			this.openEdit(view, null);
		`),
		"edit": js.Func("view, rowIdx, colIdx, item, e, rec", `
			this.openEdit(view, rec);
		`),
		"editSelected": js.Func("view", `
			let rec = view.getSelection()[0];
			if (rec) this.openEdit(view, rec);
		`),
		"remove": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let me = this;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Delete batch '{0}'?"), rec.get("namespace") || gettext("root")),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-mount-profiles/" + encodeURIComponent(encodePathValue(rec.get("id"))),
						method: "DELETE",
						waitMsgTarget: view,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: () => {
							const panel = me.getView();
							if (panel) panel.getStore().rstore.load();
						},
					});
				},
			);
		`),
		"mountNow": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/d2d-mount-profiles/" + encodeURIComponent(encodePathValue(rec.get("id"))) + "/mount",
				method: "POST",
				waitMsgTarget: view,
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
				success: (resp) => {
					Ext.create("PBS.plusWindow.TaskViewer", {
						upid: resp.result.data,
						taskDone: () => {
							const panel = me.getView();
							if (panel) panel.getStore().rstore.load();
						},
					}).show();
				},
			});
		`),
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Create", IconCls: "fa fa-plus", Handler: "add"},
		{XType: js.XButton, Text: "Edit", IconCls: "fa fa-pencil", Handler: "editSelected"},
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload"},
	},
	Columns: []js.Column{
		{Text: "Datastore", DataIndex: "datastore", Width: 120},
		{Text: "Parent NS", DataIndex: "namespace", Width: 110, Renderer: js.Func("v", `return v ? Ext.String.htmlEncode(v) : "-";`)},
		{Text: "Mode", DataIndex: "mode", Width: 60, Renderer: js.Func("v", `
			return v === "rw" ? "rw" : "ro";
		`)},
		{Text: "Target", DataIndex: "outpost", Flex: 1, Renderer: js.Func("v, meta, rec", `
			if (v) return Ext.String.htmlEncode(v + "/" + (rec.get("share-name") || v));
			let p = rec.get("mount-path");
			return p ? Ext.String.htmlEncode(p) : gettext("Automatic local root");
		`)},
		{Text: "Schedule", DataIndex: "schedule", Width: 140, Renderer: js.Func("v", `
			return v ? Ext.String.htmlEncode(v) : gettext("Always");
		`)},
		{Text: "Auto-mount", DataIndex: "auto-mount", Width: 90, Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle"></i> ' + gettext("Yes") : gettext("No");
		`)},
		{Text: "Replace", DataIndex: "replace", Width: 70, Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle"></i> ' + gettext("Yes") : gettext("No");
		`)},
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "id", Width: 110, Items: js.Arr{
			js.Obj{
				"handler": "mountNow",
				"tooltip": js.T("Mount the newest snapshots now"),
				"getClass": js.Func("v, meta, rec", `
					return "fa fa-fw fa-play";
				`),
			},
			js.Obj{
				"handler":  "edit",
				"tooltip":  js.T("Edit"),
				"getClass": js.Func("v, meta, rec", `return "fa fa-fw fa-pencil";`),
			},
			js.Obj{
				"handler":  "remove",
				"tooltip":  js.T("Delete"),
				"getClass": js.Func("v, meta, rec", `return "fa fa-fw fa-trash";`),
			},
		}},
	},
}
