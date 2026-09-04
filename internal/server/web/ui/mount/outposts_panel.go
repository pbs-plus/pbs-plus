package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var outpostsModel = js.Model{
	Name:       "pbs-model-outposts",
	Fields:     js.Fields("name", "type", "listen-addr", "sectype", "running", "error", "attached", "endpoints"),
	IDProperty: "name",
}

var outpostsPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.OutpostsPanel", XType: "pbsPlusOutpostsPanel",
	Title: "Outposts",
	Store: js.Store{StoreID: "pbs-plus-outposts", Model: "pbs-model-outposts", APIPath: "/api2/extjs/config/d2d-outposts", Sorters: "name"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
		`),
		"reload": js.Func("", `
			this.getView().getStore().load();
		`),
		"add": js.Func("", `
			this.openEdit(null);
		`),
		"editSelected": js.Func("", `
			let view = this.getView();
			let rec = view.getSelection()[0];
			if (!rec) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select an outpost."));
				return;
			}
			this.openEdit(rec);
		`),
		"openEdit": js.Func("rec", `
			let isEdit = !!rec;
			let values = isEdit ? rec.data : {};
			let panel = this.getView();
			Ext.create("Ext.window.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit Outpost '{0}'"), values.name) : gettext("Add Outpost"),
				width: 460,
				modal: true,
				bodyPadding: 10,
				items: [{
					xtype: "form",
					anchor: "100%",
					border: false,
					defaults: { anchor: "100%", labelWidth: 120 },
					items: [
						{
							xtype: "proxmoxtextfield",
							name: "name",
							fieldLabel: gettext("Name"),
							allowBlank: false,
							regex: /^[a-z0-9][a-z0-9-]{0,31}$/,
							regexText: gettext("Lowercase letters, digits and dashes"),
							value: values.name,
							readOnly: isEdit,
						},
						{
							xtype: "combobox",
							name: "type",
							fieldLabel: gettext("Type"),
							store: [
								["nfs", "NFSv3 (built-in)"],
								["ganesha", "NFS (Ganesha, Kerberos)"],
								["samba", "SMB (Samba)"],
							],
							value: values.type || "nfs",
							editable: false,
							allowBlank: false,
							listeners: {
								change: (f, v) => {
									let form = f.up("form");
									let listen = form.down("[name=listen-addr]");
									let sectype = form.down("[name=sectype]");
									if (listen) listen.setDisabled(v !== "nfs");
									if (sectype) sectype.setDisabled(v !== "ganesha");
								},
							},
						},
						{
							xtype: "proxmoxtextfield",
							name: "listen-addr",
							fieldLabel: gettext("Listen Address"),
							emptyText: "0.0.0.0:2049",
							allowBlank: false,
							value: values["listen-addr"],
							disabled: values.type && values.type !== "nfs",
						},
						{
							xtype: "combobox",
							name: "sectype",
							fieldLabel: gettext("SecType"),
							store: [["krb5i", "krb5i"], ["krb5p", "krb5p"], ["krb5,krb5i", "krb5,krb5i"], ["sys", "sys (no auth)"]],
							value: values.sectype || "krb5i",
							editable: false,
							disabled: !values.type || values.type !== "ganesha",
						},
						{
							xtype: "displayfield",
							value: gettext("Ganesha and Samba outposts require the service installed and running; shares are exported from private mounts. The built-in NFSv3 outpost has no per-user authentication: restrict network access to trusted hosts."),
						},
					],
				}],
				buttons: [
					{
						text: isEdit ? gettext("Save") : gettext("Create"),
						handler: (btn) => {
							let w = btn.up("window");
							let form = w.down("form");
							if (!form.isValid()) return;
							let vals = form.getValues();
							let params = {
								name: vals.name,
								type: vals.type,
								"listen-addr": vals["listen-addr"] || "",
								sectype: vals.sectype || "",
							};
							let url = "/api2/extjs/config/d2d-outposts";
							let method = "POST";
							if (isEdit) {
								method = "PUT";
								url += "/" + encodeURIComponent(vals.name);
							}
							PBS.PlusUtils.API2Request({
								url,
								method,
								params,
								waitMsgTarget: w,
								failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
								success: () => {
									w.close();
									panel.getStore().load();
								},
							});
						},
					},
				],
			}).show();
		`),
		"remove": js.Func("table, rec, el, rowIdx, colIdx, item, e, rec", `
			let panel = this.getView();
			Ext.Msg.confirm(
				gettext("Remove Outpost"),
				Ext.String.format(gettext("Remove outpost {0}?"), rec.data.name),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-outposts/" + encodeURIComponent(rec.data.name),
						method: "DELETE",
						waitMsgTarget: panel,
						failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
						success: () => panel.getStore().load(),
					});
				},
			);
		`),
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Create", IconCls: "fa fa-plus", Handler: "add"},
		{XType: js.XButton, Text: "Edit", IconCls: "fa fa-pencil", Handler: "editSelected"},
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload"},
	},
	Columns: []js.Column{
		{Text: "Name", DataIndex: "name", Width: 140},
		{Text: "Type", DataIndex: "type", Width: 130, Renderer: js.Func("v", `
			if (v === "nfs") return "NFSv3";
			if (v === "ganesha") return "NFS (Ganesha)";
			if (v === "samba") return "SMB (Samba)";
			return Ext.String.htmlEncode(v || "");
		`)},
		{Text: "SecType", DataIndex: "sectype", Width: 90, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "-");`)},
		{Text: "Listen Address", DataIndex: "listen-addr", Width: 160, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Status", DataIndex: "running", Width: 90, Renderer: js.Func("v, meta, rec", `
			if (v) return '<i class="fa fa-check-circle"></i> ' + gettext("Running");
			return '<i class="fa fa-times-circle"></i> ' + gettext("Stopped");
		`)},
		{Text: "Shares", DataIndex: "attached", Flex: 1, Renderer: js.Func("v, meta, rec", `
			let shares = v || [];
			let endpoints = rec.get("endpoints") || [];
			if (!shares.length) return "-";
			return Ext.String.htmlEncode(shares.length + " (" + endpoints.join(", ") + ")");
		`)},
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "name", Width: 90, Items: js.Arr{
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
