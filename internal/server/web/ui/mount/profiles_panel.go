package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var mountProfilesModel = js.Model{
	Name:       "pbs-model-mount-profiles",
	Fields:     js.Fields("id", "datastore", "namespace", "mode", "outpost", "share-name", "mount-path", "schedule", "auto-mount", "replace", "layout"),
	IDProperty: "id",
}

var mountProfilesPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.ProfilesPanel", XType: "pbsPlusMountProfilesPanel",
	Title:     "Mount Profiles",
	Store:     js.Store{StoreID: "pbs-plus-mount-profiles", Model: "pbs-model-mount-profiles", Interval: 5000, APIPath: "/api2/extjs/config/d2d-mount-profiles", Sorters: "id"},
	Listeners: js.Listeners{Activate: "startStore", Deactivate: "stopStore", BeforeDestroy: "stopStore"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			PBS.PlusUtils.monStoreErrors(view, view.getStore().rstore);
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
		"profileParams": js.Func("form", `
			let vals = form.getValues();
			let fieldset = form.down("#pathLayout");
			let layout = null;
			if (!fieldset.collapsed) {
				layout = {
					template: vals["layout-template"] || "",
					fallback: vals["layout-fallback"] || "",
					selection: vals["layout-selection"] || "group",
					resolver_url: vals["layout-resolver"] || "",
					resolver_token_env: vals["layout-token-env"] || "",
					windows_names: vals["layout-windows"] === "1",
				};
			}
			return {
				datastore: vals.datastore, ns: vals.ns || "", mode: vals.mode,
				outpost: vals.outpost || "", "share-name": vals["share-name"] || "",
				"mount-path": vals["mount-path"] || "", schedule: vals.schedule || "",
				"auto-mount": vals["auto-mount"] === "1" ? 1 : 0,
				replace: vals.replace === "1" ? 1 : 0,
				layout: layout ? JSON.stringify(layout) : "",
			};
		`),
		"openEdit": js.Func("view, rec", `
			let isEdit = !!rec;
			let values = isEdit ? rec.data : {};
			let layout = values.layout || {};
			let win = Ext.create("PBS.plusWindow.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit Batch '{0}'"), values.namespace || gettext("root")) : gettext("Add Mount Batch"),
				width: 640,
				modal: true,
				bodyPadding: 10,
				items: [{
					xtype: "form",
					anchor: "100%",
					border: false,
					defaults: { anchor: "100%", labelWidth: 120 },
					items: [
					{
						xtype: "pbsDataStoreSelector",
						name: "datastore",
						fieldLabel: gettext("Datastore"),
						allowBlank: false,
						value: values.datastore,
						listeners: {
							change: (cb, v) => {
								let win = cb.up("window");
								let nsCombo = win.down("pbsNamespaceSelector[name=ns]");
								if (nsCombo) {
									nsCombo.setDatastore(v);
								}
							},
						},
					},
					{
						xtype: "pbsNamespaceSelector",
						name: "ns",
						fieldLabel: gettext("Parent Namespace"),
						datastore: values.datastore,
						emptyText: gettext("root (all namespaces)"),
						value: values.namespace,
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
							xtype: "fieldset", itemId: "pathLayout", title: gettext("Custom directory layout (read-only)"),
							checkboxToggle: true, collapsed: !values.layout,
							defaults: { anchor: "100%", labelWidth: 150 },
							items: [
								{ xtype: "combobox", name: "layout-selection", fieldLabel: gettext("Snapshot selection"),
									store: [["group", gettext("Newest per backup group")], ["namespace", gettext("Newest across groups per namespace")]],
									queryMode: "local", editable: false, forceSelection: true, value: layout.selection || "group" },
								{ xtype: "proxmoxtextfield", name: "layout-template", fieldLabel: gettext("Path template"),
									value: layout.template || "{ns.rel}/{type}-{id}" },
								{ xtype: "displayfield", value: gettext("Tokens: {ns}, {ns.rel}, {ns.leaf}, {type}, {id}, {archive}, {time}, {resolve}. Paths are relative to the local root or export bucket/share. Colliding paths are rejected.") },
								{ xtype: "proxmoxtextfield", name: "layout-resolver", fieldLabel: gettext("HTTP resolver URL"),
									value: layout.resolver_url || "", emptyText: gettext("Optional HTTP(S) endpoint") },
								{ xtype: "proxmoxtextfield", name: "layout-token-env", fieldLabel: gettext("Bearer token variable"),
									value: layout.resolver_token_env || "", emptyText: gettext("Server environment variable name, not the token") },
								{ xtype: "proxmoxtextfield", name: "layout-fallback", fieldLabel: gettext("Empty result fallback"),
									value: layout.fallback || "", emptyText: "Uncategorized/{ns.leaf}" },
								{ xtype: "proxmoxcheckbox", name: "layout-windows", fieldLabel: gettext("Windows-safe names"),
									inputValue: "1", uncheckedValue: "0", checked: !!layout.windows_names },
								{ xtype: "displayfield", value: gettext('The resolver receives snapshot metadata by JSON POST and returns {"values":["Project A","Project B"]}. Use {resolve} in the template. Each value is a folder name; multiple values expose one snapshot at multiple paths. Empty values use the fallback. Resolver errors preserve current mounts. Bearer authentication requires HTTPS.') },
							],
						},
						{ xtype: "displayfield", itemId: "layoutPreview", hidden: true },
						{ xtype: "displayfield", value: gettext("Without a custom layout, mounts the newest snapshot per backup group using the existing namespace directory layout. NFS and S3 snapshot exports use the same directory planner.") },

					],
				}],
				buttons: [
					{
						text: gettext("Preview paths"),
						handler: (btn) => {
							let w = btn.up("window");
							let form = w.down("form");
							if (!form.isValid()) return;
							PBS.PlusUtils.API2Request({
								url: "/api2/extjs/config/d2d-mount-profiles/preview", method: "POST",
								params: this.profileParams(form), waitMsgTarget: w,
								failure: (resp) => { if (!w.destroyed) Ext.Msg.alert(gettext("Error"), resp.htmlStatus); },
								success: (resp) => {
									if (w.destroyed) return;
									let plans = resp.result.data || [];
									let lines = [];
									for (let plan of plans) {
										for (let path of plan.paths) {
											if (lines.length >= 100) break;
											lines.push(path + "  [" + plan.backup_time + "]");
										}
										if (lines.length >= 100) break;
									}
									let preview = form.down("#layoutPreview");
									preview.setValue('<pre style="max-height:240px;overflow:auto;white-space:pre-wrap">' + Ext.String.htmlEncode(lines.join("\n") || gettext("No mountable snapshots")) + "</pre>");
									preview.show();
								},
							});
						},
					},
					{
						text: isEdit ? gettext("Save") : gettext("Create"),
						handler: (btn) => {
							let me = this;
							let w = btn.up("window");
							let form = w.down("form");
							if (!form.isValid()) return;
							let params = this.profileParams(form);
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
		"editSelected": js.Func("btn", `
			let view = this.getView();
			let rec = view.getSelectionModel().getSelection()[0];
			if (!rec) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select a profile."));
				return;
			}
			this.openEdit(view, rec);
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
