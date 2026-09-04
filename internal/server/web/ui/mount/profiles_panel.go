package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var mountProfilesModel = js.Model{
	Name:       "pbs-model-mount-profiles",
	Fields:     js.Fields("id", "datastore", "namespace", "backup-type", "backup-id", "mode", "backend", "mount-path", "schedule", "auto-mount"),
	IDProperty: "id",
}

var mountProfilesPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.ProfilesPanel", XType: "pbsPlusMountProfilesPanel",
	Title: "Mount Profiles",
	Store: js.Store{StoreID: "pbs-plus-mount-profiles", Model: "pbs-model-mount-profiles", APIPath: "/api2/extjs/config/d2d-mount-profiles", Sorters: "id"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
		`),
		"reload": js.Func("", `
			this.getView().getStore().load();
		`),
		"openEdit": js.Func("view, rec", `
			let isEdit = !!rec;
			let values = isEdit ? rec.data : {};
			let win = Ext.create("Ext.window.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit Profile '{0}'"), values["backup-type"] + "/" + values["backup-id"]) : gettext("Add Mount Profile"),
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
					},
					{
						xtype: "proxmoxtextfield",
						name: "ns",
						fieldLabel: gettext("Namespace"),
						emptyText: gettext("root"),
						value: values.namespace,
					},
					{
						xtype: "combobox",
						name: "backup-type",
						fieldLabel: gettext("Backup Type"),
						store: [["host", "host"], ["vm", "vm"], ["ct", "ct"]],
						allowBlank: false,
						value: values["backup-type"] || "host",
					},
					{
						xtype: "proxmoxtextfield",
						name: "backup-id",
						fieldLabel: gettext("Backup ID"),
						allowBlank: false,
						value: values["backup-id"],
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
						name: "backend",
						fieldLabel: gettext("Backend"),
						store: [["fuse", "FUSE"], ["nfs", "NFSv3"]],
						value: values.backend || "fuse",
						editable: false,
					},
					{
						xtype: "proxmoxtextfield",
						name: "mount-path",
						fieldLabel: gettext("Mount Path"),
						emptyText: gettext("Automatic (under /mnt/pbs-plus-restores)"),
						value: values["mount-path"],
					},
					{
						xtype: "proxmoxtextfield",
						name: "schedule",
						fieldLabel: gettext("Check Schedule"),
						emptyText: gettext("Always (checked every 5 minutes)"),
						value: values.schedule,
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
							xtype: "displayfield",
							value: gettext("The profile always mounts the newest snapshot of the group."),
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
								"backup-type": vals["backup-type"],
								"backup-id": vals["backup-id"],
								mode: vals.mode,
								backend: vals.backend,
									outpost: vals.outpost || "",
								"mount-path": vals["mount-path"] || "",
								"schedule": vals.schedule || "",
								"auto-mount": vals["auto-mount"] === "1" ? 1 : 0,
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
									if (panel) panel.getStore().load();
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
				Ext.String.format(gettext("Delete profile for '{0}/{1}'?"), rec.get("backup-type"), rec.get("backup-id")),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-mount-profiles/" + encodeURIComponent(encodePathValue(rec.get("id"))),
						method: "DELETE",
						waitMsgTarget: view,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: () => {
							const panel = me.getView();
							if (panel) panel.getStore().load();
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
							if (panel) panel.getStore().load();
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
		{Text: "Namespace", DataIndex: "namespace", Width: 110, Renderer: js.Func("v", `return v ? Ext.String.htmlEncode(v) : "-";`)},
		{Text: "Group", DataIndex: "backup-id", Flex: 1, Renderer: js.Func("v, meta, rec", `
			return Ext.String.htmlEncode(rec.get("backup-type") + "/" + v);
		`)},
		{Text: "Mode", DataIndex: "mode", Width: 60, Renderer: js.Func("v", `
			return v === "rw" ? "rw" : "ro";
		`)},
		{Text: "Backend", DataIndex: "backend", Width: 75, Renderer: js.Func("v", `return v === "nfs" ? "NFSv3" : "FUSE";`)},
		{Text: "Mount Path", DataIndex: "mount-path", Flex: 1, Renderer: js.Func("v", `
			return v ? Ext.String.htmlEncode(v) : gettext("Automatic");
		`)},
		{Text: "Schedule", DataIndex: "schedule", Width: 140, Renderer: js.Func("v", `
			return v ? Ext.String.htmlEncode(v) : gettext("Always");
		`)},
		{Text: "Auto-mount", DataIndex: "auto-mount", Width: 90, Renderer: js.Func("v", `
			return v ? '<i class="fa fa-check-circle"></i> ' + gettext("Yes") : gettext("No");
		`)},
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "id", Width: 110, Items: js.Arr{
			js.Obj{
				"handler": "mountNow",
				"tooltip": js.T("Mount the newest snapshot now"),
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
