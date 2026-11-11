package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var outpostsModel = js.Model{
	Name:       "pbs-model-outposts",
	Fields:     js.Fields("name", "type", "listen-addr", "guest", "valid-users", "force-user", "hosts-allow", "browseable", "running", "error", "attached", "endpoints", "s3-export"),
	IDProperty: "name",
}

var outpostsPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.OutpostsPanel", XType: "pbsPlusOutpostsPanel",
	Title:     "Outposts",
	Store:     js.Store{StoreID: "pbs-plus-outposts", Model: "pbs-model-outposts", Interval: 5000, APIPath: "/api2/extjs/config/d2d-outposts", Sorters: "name"},
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
		"add": js.Func("", `
			this.openEdit(null);
		`),
		"editSelected": js.Func("", `
			let view = this.getView();
			let rec = view.getSelectionModel().getSelection()[0];
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
			let exportConfig = Ext.clone(values["s3-export"] || {});
			let exportCredentials = Ext.create("Ext.data.Store", {
				fields: ["access_key", "secret_key"],
				data: exportConfig.credentials || [],
			});
			let openExportCredentialEditor = () => {
				let editor = Ext.create("PBS.plusWindow.Window", {
					title: gettext("Add Credential"),
					width: 460,
					modal: true,
					bodyPadding: 10,
					items: [{
						xtype: "form",
						border: false,
						defaults: { anchor: "100%", labelWidth: 120 },
						items: [
							{ xtype: "proxmoxtextfield", name: "access-key", fieldLabel: gettext("Access Key"), allowBlank: false, minLength: 3 },
							{ xtype: "proxmoxtextfield", name: "secret-key", fieldLabel: gettext("Secret Key"), inputType: "password", allowBlank: false, minLength: 8 },
						],
					}],
					buttons: [{
						text: gettext("Add"),
						handler: () => {
							let form = editor.down("form").getForm();
							if (!form.isValid()) return;
							let vals = form.getValues();
							if (exportCredentials.findExact("access_key", vals["access-key"]) !== -1) {
								Ext.Msg.alert(gettext("Error"), gettext("Access keys must be unique."));
								return;
							}
							exportCredentials.add({ access_key: vals["access-key"], secret_key: vals["secret-key"] });
							editor.close();
						},
					}],
				});
				editor.show();
			};
			let win = Ext.create("PBS.plusWindow.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit Outpost '{0}'"), values.name) : gettext("Add Outpost"),
				width: 520,
				modal: true,
				bodyPadding: 10,
				items: [{
					xtype: "form",
					anchor: "100%",
					border: false,
					autoScroll: true,
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
								["s3-export", "S3 Snapshot Export (read-only)"],
							],
							value: values.type || "nfs",
							editable: false,
							allowBlank: false,
							listeners: {
								change: (f, v) => {
									let form = f.up("form");
									let listen = form.down("[name=listen-addr]");
									let smb = form.down("[itemId=sambaFields]");
									let exportFields = form.down("[itemId=s3ExportFields]");
									if (!isEdit && listen) {
										if (v === "s3-export" && listen.getValue() === "0.0.0.0:2049") listen.setValue("0.0.0.0:9100");
										if (v === "nfs" && listen.getValue() === "0.0.0.0:9100") listen.setValue("0.0.0.0:2049");
									}
									if (exportFields) {
										exportFields.setVisible(v === "s3-export");
										exportFields.setDisabled(v !== "s3-export");
									}
									if (listen) {
										listen.setVisible(v !== "samba");
										listen.setDisabled(v === "samba");
									}
									if (smb) {
										smb.setVisible(v === "samba");
										smb.setDisabled(v !== "samba");
									}
								},
							},
						},
						{
							xtype: "proxmoxtextfield",
							name: "listen-addr",
							fieldLabel: gettext("Listen Address"),
							emptyText: "0.0.0.0:2049",
							allowBlank: false,
							value: values["listen-addr"] || (values.type === "s3-export" ? "0.0.0.0:9100" : "0.0.0.0:2049"),
							hidden: values.type === "samba",
							disabled: values.type === "samba",
						},
						{
							xtype: "container",
							itemId: "sambaFields",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "samba",
							disabled: values.type !== "samba",
							items: [
								{
									xtype: "proxmoxcheckbox",
									name: "guest",
									fieldLabel: gettext("Allow Guests"),
									uncheckedValue: "0",
									inputValue: "1",
									value: values.guest,
									boxLabel: gettext("Anonymous access, no password"),
								},
								{
									xtype: "proxmoxtextfield",
									name: "valid-users",
									fieldLabel: gettext("Valid Users"),
									emptyText: "DOMAIN\\restore-ops, @DOMAIN\\backup-admins",
									value: values["valid-users"],
								},
								{
									xtype: "proxmoxtextfield",
									name: "force-user",
									fieldLabel: gettext("Force User"),
									emptyText: "root",
									value: values["force-user"],
								},
								{
									xtype: "proxmoxtextfield",
									name: "hosts-allow",
									fieldLabel: gettext("Hosts Allow"),
									emptyText: "10.0.0.0/8, 192.168.1.",
									value: values["hosts-allow"],
								},
								{
									xtype: "proxmoxcheckbox",
									name: "browseable",
									fieldLabel: gettext("Browseable"),
									uncheckedValue: "0",
									inputValue: "1",
									value: values.browseable,
									boxLabel: gettext("List share names when clients enumerate the server"),
								},
							],
						},
						{
							xtype: "container",
							itemId: "s3ExportFields",
							layout: "anchor",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "s3-export",
							disabled: values.type !== "s3-export",
							items: [
								{
									xtype: "combobox",
									name: "export-region",
									fieldLabel: gettext("Region"),
									store: ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-central-1", "eu-west-1", "eu-west-2", "ap-southeast-1", "ap-northeast-1", "sa-east-1"],
									queryMode: "local",
									editable: true,
									forceSelection: false,
									value: exportConfig.region || "us-east-1",
								},
								{
									xtype: "proxmoxcheckbox",
									name: "export-tls",
									fieldLabel: gettext("HTTPS"),
									boxLabel: gettext("Serve HTTPS using the current PBS certificate"),
									inputValue: "1",
									uncheckedValue: "0",
									checked: exportConfig.tls !== false,
									listeners: {
										change: (field, enabled) => {
											let form = field.up("form");
											for (let name of ["export-tls-cert", "export-tls-key"]) form.down("[name=" + name + "]").setDisabled(!enabled);
										},
									},
								},
								{
									xtype: "proxmoxtextfield",
									name: "export-tls-cert",
									fieldLabel: gettext("Custom Certificate"),
									emptyText: gettext("Current PBS certificate (default)"),
									value: exportConfig["tls-cert"],
									disabled: exportConfig.tls === false,
								},
								{
									xtype: "proxmoxtextfield",
									name: "export-tls-key",
									fieldLabel: gettext("Custom Key"),
									emptyText: gettext("Current PBS key (default)"),
									value: exportConfig["tls-key"],
									disabled: exportConfig.tls === false,
								},
								{
									xtype: "grid",
									itemId: "exportCredentialGrid",
									title: gettext("Credentials"),
									height: 160,
									margin: "0 0 10 0",
									store: exportCredentials,
									columns: [
										{ text: gettext("Access Key"), dataIndex: "access_key", flex: 1 },
										{ text: gettext("Secret Key"), dataIndex: "secret_key", width: 110, renderer: (v) => v ? gettext("New") : gettext("Stored") },
									],
									tbar: [
										{ text: gettext("Add"), iconCls: "fa fa-plus", handler: () => openExportCredentialEditor() },
										{ text: gettext("Remove"), iconCls: "fa fa-trash", handler: (btn) => {
											let selected = btn.up("grid").getSelectionModel().getSelection()[0];
											if (selected) exportCredentials.remove(selected);
										} },
									],
								},
								{
									xtype: "displayfield",
									value: gettext("Each attached read-only mount becomes one bucket, named after its share in lowercase. Every credential can read every bucket. Read-write mounts cannot be attached."),
								},
							],
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
							if (vals.type === "s3-export") {
								if (!exportCredentials.getCount()) {
									Ext.Msg.alert(gettext("Error"), gettext("Add at least one credential."));
									return;
								}
								let tlsEnabled = vals["export-tls"] === "1";
								let cert = vals["export-tls-cert"] || "";
								let key = vals["export-tls-key"] || "";
								if (tlsEnabled && (!!cert !== !!key)) {
									Ext.Msg.alert(gettext("Error"), gettext("Set both the custom TLS certificate and key, or leave both empty."));
									return;
								}
								let exportParams = {
									region: vals["export-region"] || "us-east-1",
									tls: tlsEnabled,
									credentials: exportCredentials.getRange().map((row) => ({ access_key: row.get("access_key"), secret_key: row.get("secret_key") || "" })),
								};
								if (tlsEnabled && cert) exportParams["tls-cert"] = cert;
								if (tlsEnabled && key) exportParams["tls-key"] = key;
								params["s3-export"] = JSON.stringify(exportParams);
							}
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
									panel.getStore().rstore.load();
								},
							});
						},
					},
				],
			});
			win.show();
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
						success: () => panel.getStore().rstore.load(),
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
			if (v === "s3-export") return "S3 Export";
			return Ext.String.htmlEncode(v || "");
		`)},
		{Text: "Listen Address", DataIndex: "listen-addr", Width: 160, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Access", DataIndex: "valid-users", Width: 180, Renderer: js.Func("v, meta, rec", `
			if (rec.get("type") === "s3-export") {
				let credentials = (rec.get("s3-export") || {}).credentials || [];
				return Ext.String.format(gettext("{0} credential(s)"), credentials.length);
			}
			if (rec.get("type") !== "samba") return "-";
			if (rec.get("guest")) return gettext("Guest");
			return Ext.String.htmlEncode(v || "-");
		`)},
		{Text: "Status", DataIndex: "running", Width: 100, Renderer: js.Func("v, meta, rec", `
			if (v) return '<i class="fa fa-check-circle"></i> ' + gettext("Running");
			let error = rec.get("error") || "";
			if (error) meta.tdAttr = 'data-qtip="' + Ext.String.htmlEncode(error) + '"';
			return '<i class="fa fa-times-circle"></i> ' + gettext("Stopped");
		`)},
		{Text: "Resources", DataIndex: "attached", Flex: 1, Renderer: js.Func("v, meta, rec", `
			let endpoints = rec.get("endpoints") || [];
			let shares = v || [];
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
