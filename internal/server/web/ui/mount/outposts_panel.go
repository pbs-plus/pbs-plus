package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var outpostsModel = js.Model{
	Name:       "pbs-model-outposts",
	Fields:     js.Fields("name", "type", "listen-addr", "guest", "valid-users", "force-user", "hosts-allow", "browseable", "running", "error", "attached", "endpoints"),
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
								["samba", "SMB (Samba)"],
							],
							value: values.type || "nfs",
							editable: false,
							allowBlank: false,
							listeners: {
								change: (f, v) => {
									let form = f.up("form");
									let listen = form.down("[name=listen-addr]");
									let smb = v === "samba";
									if (listen) listen.setDisabled(v !== "nfs");
									["guest", "valid-users", "force-user", "hosts-allow", "browseable"].forEach((n) => {
										let fld = form.down("[name=" + n + "]");
										if (fld) fld.setDisabled(!smb);
									});
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
							xtype: "proxmoxcheckbox",
							name: "guest",
							fieldLabel: gettext("Allow Guests"),
							uncheckedValue: "0",
							inputValue: "1",
							value: values.guest,
							disabled: values.type !== "samba",
							boxLabel: gettext("Anonymous access, no password"),
						},
						{
							xtype: "proxmoxtextfield",
							name: "valid-users",
							fieldLabel: gettext("Valid Users"),
							emptyText: "DOMAIN\\restore-ops, @DOMAIN\\backup-admins",
							value: values["valid-users"],
							disabled: values.type !== "samba",
						},
						{
							xtype: "proxmoxtextfield",
							name: "force-user",
							fieldLabel: gettext("Force User"),
							emptyText: "root",
							value: values["force-user"],
							disabled: values.type !== "samba",
						},
						{
							xtype: "proxmoxtextfield",
							name: "hosts-allow",
							fieldLabel: gettext("Hosts Allow"),
							emptyText: "10.0.0.0/8, 192.168.1.",
							value: values["hosts-allow"],
							disabled: values.type !== "samba",
						},
						{
							xtype: "proxmoxcheckbox",
							name: "browseable",
							fieldLabel: gettext("Browseable"),
							uncheckedValue: "0",
							inputValue: "1",
							value: values.browseable,
							disabled: values.type !== "samba",
							boxLabel: gettext("List share names when clients enumerate the server"),
						},
						{
							xtype: "displayfield",
							value: gettext("Samba outposts need smbd running with 'include' pointing at the pbs-plus outpost config. Set either guest access or valid users. Domain accounts (DOMAIN\\user) require the host to be joined with 'net ads join'. Read-only shares preserve backed-up ownership. Writable shares with Force User map pxar ownership to that NSS/winbind account while retaining source mode and ACL checks. The built-in NFSv3 outpost has no per-user authentication: restrict network access to trusted hosts."),
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
								guest: vals.guest || "0",
								"valid-users": vals["valid-users"] || "",
								"force-user": vals["force-user"] || "",
								"hosts-allow": vals["hosts-allow"] || "",
								browseable: vals.browseable || "0",
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
		"edit": js.Func("view, rowIdx, colIdx, item, e, rec", `
			this.openEdit(rec);
		`),
		"remove": js.Func("view, rowIdx, colIdx, item, e, rec", `
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
			if (v === "samba") return "SMB (Samba)";
			return Ext.String.htmlEncode(v || "");
		`)},
		{Text: "Listen Address", DataIndex: "listen-addr", Width: 160, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Access", DataIndex: "valid-users", Width: 180, Renderer: js.Func("v, meta, rec", `
			if (rec.get("type") !== "samba") return "-";
			if (rec.get("guest")) return gettext("Guest");
			return Ext.String.htmlEncode(v || "-");
		`)},
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
