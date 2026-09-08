package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var outpostsModel = js.Model{
	Name:       "pbs-model-outposts",
	Fields:     js.Fields("name", "type", "listen-addr", "guest", "valid-users", "force-user", "hosts-allow", "browseable", "running", "error", "attached", "endpoints", "s3"),
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
			let s3cfg = values.s3 || {};
			let s3bucket = (s3cfg.buckets && s3cfg.buckets[0]) || {};
			let s3cred = (s3cfg.credentials && s3cfg.credentials[0]) || {};
			let s3grant = {};
			if (s3cred.grants) {
				for (let grant of s3cred.grants) {
					if (grant.bucket === s3bucket.name) { s3grant = grant; break; }
				}
			}
			let complexS3 = !!(s3cfg.buckets && s3cfg.buckets.length > 1) || !!(s3cfg.credentials && s3cfg.credentials.length > 1);
			let win = Ext.create("Ext.window.Window", {
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
								["s3", "S3 (objects as snapshots)"],
							],
							value: values.type || "nfs",
							editable: false,
							allowBlank: false,
							listeners: {
								change: (f, v) => {
									let form = f.up("form");
									let listen = form.down("[name=listen-addr]");
									let smb = form.down("[itemId=sambaFields]");
									let structured = form.down("[itemId=s3Fields]");
									let raw = form.down("[itemId=s3JsonFields]");
									if (listen) listen.setVisible(v !== "samba");
									if (smb) smb.setVisible(v === "samba");
									if (structured) structured.setVisible(v === "s3" && !complexS3);
									if (raw) raw.setVisible(v === "s3" && complexS3);
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
							hidden: values.type === "samba",
						},
						{
							xtype: "container",
							itemId: "sambaFields",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "samba",
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
							itemId: "s3Fields",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "s3" || complexS3,
							items: [
								{
									xtype: "fieldset",
									title: gettext("Bucket"),
									items: [
										{
											xtype: "proxmoxtextfield",
											name: "bucket",
											fieldLabel: gettext("Bucket Name"),
											allowBlank: false,
											regex: /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/,
											regexText: gettext("3-63 lowercase letters, digits, dots and dashes"),
											value: s3bucket.name,
										},
										{
											xtype: "combobox",
											name: "datastore",
											fieldLabel: gettext("Datastore"),
											store: "pbs-datastore-list",
											displayField: "store",
											valueField: "store",
											allowBlank: false,
											value: s3bucket.datastore,
											listeners: {
												change: (cb, v) => {
													let nsCombo = cb.up("form").down("pbsNamespaceSelector[name=ns]");
													if (nsCombo) nsCombo.setDatastore(v);
												},
											},
										},
										{
											xtype: "pbsNamespaceSelector",
											name: "ns",
											fieldLabel: gettext("Namespace"),
											datastore: s3bucket.datastore,
											emptyText: gettext("root"),
											value: s3bucket.namespace,
										},
										{
											xtype: "combobox",
											name: "backup-type",
											fieldLabel: gettext("Backup Type"),
											store: [["host", gettext("Host")], ["vm", gettext("VM")], ["ct", gettext("Container")]],
											value: s3bucket.backup_type || "host",
											editable: false,
											allowBlank: false,
										},
										{
											xtype: "proxmoxtextfield",
											name: "backup-id",
											fieldLabel: gettext("Backup ID"),
											allowBlank: false,
											regex: /^[A-Za-z0-9_][A-Za-z0-9._-]*$/,
											regexText: gettext("Letters, digits, dots, dashes and underscores; start with a letter or digit"),
											value: s3bucket.backup_id,
										},
									],
								},
								{
									xtype: "fieldset",
									title: gettext("Credential"),
									items: [
										{
											xtype: "proxmoxtextfield",
											name: "access-key",
											fieldLabel: gettext("Access Key"),
											allowBlank: false,
											value: s3cred.access_key,
										},
										{
											xtype: "proxmoxtextfield",
											name: "secret-key",
											fieldLabel: gettext("Secret Key"),
											inputType: "password",
											allowBlank: false,
											minLength: 8,
											value: s3cred.secret_key,
										},
										{
											xtype: "proxmoxtextfield",
											name: "auth-id",
											fieldLabel: gettext("Owner"),
											allowBlank: false,
											regex: /^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+(![A-Za-z0-9._-]+)?$/,
											regexText: gettext("PBS auth id, e.g. root@pam"),
											value: s3cred.auth_id || "root@pam",
										},
										{
											xtype: "checkboxgroup",
											fieldLabel: gettext("Permissions"),
											items: [
												{ boxLabel: gettext("Read"), name: "read", inputValue: "1", uncheckedValue: "0", checked: isEdit ? !!s3grant.read : true },
												{ boxLabel: gettext("Write"), name: "write", inputValue: "1", uncheckedValue: "0", checked: isEdit ? !!s3grant.write : true },
												{ boxLabel: gettext("Delete"), name: "delete", inputValue: "1", uncheckedValue: "0", checked: isEdit ? !!s3grant.delete : true },
											],
										},
									],
								},
								{
									xtype: "fieldset",
									title: gettext("Advanced"),
									collapsible: true,
									collapsed: !(s3cfg["tls-cert"] || s3cfg["tls-key"] || s3cfg["spool-dir"]),
									items: [
										{
											xtype: "combobox",
											name: "region",
											fieldLabel: gettext("Region"),
											store: ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-central-1", "eu-west-1", "eu-west-2", "ap-southeast-1", "ap-northeast-1", "sa-east-1"],
											queryMode: "local",
											editable: true,
											forceSelection: false,
											value: s3cfg.region || "us-east-1",
										},
										{
											xtype: "proxmoxtextfield",
											name: "tls-cert",
											fieldLabel: gettext("TLS Certificate"),
											emptyText: "/etc/proxmox-backup/pbs-plus/certs/server.crt",
											value: s3cfg["tls-cert"],
										},
										{
											xtype: "proxmoxtextfield",
											name: "tls-key",
											fieldLabel: gettext("TLS Key"),
											emptyText: "/etc/proxmox-backup/pbs-plus/certs/server.key",
											value: s3cfg["tls-key"],
										},
										{
											xtype: "proxmoxtextfield",
											name: "spool-dir",
											fieldLabel: gettext("Spool Directory"),
											emptyText: gettext("inside the datastore (default)"),
											value: s3cfg["spool-dir"],
										},
									],
								},
								{
									xtype: "displayfield",
									value: gettext("Objects become snapshots in the mapped backup group; clients authenticate with SigV4 access keys."),
								},
							],
						},
						{
							xtype: "container",
							itemId: "s3JsonFields",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "s3" || !complexS3,
							items: [
								{
									xtype: "displayfield",
									value: gettext("This outpost maps multiple buckets or credentials; edit the raw S3 JSON."),
								},
								{
									xtype: "textarea",
									name: "s3",
									fieldLabel: gettext("S3 Config (JSON)"),
									height: 240,
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
							if (vals.type === "s3") {
								if (complexS3) {
									params.s3 = vals.s3 || "";
									try { JSON.parse(params.s3); } catch (err) {
										Ext.Msg.alert(gettext("Error"), gettext("S3 config is not valid JSON."));
										return;
									}
								} else {
									let s3 = {
										region: vals.region || "",
										buckets: [{
											name: vals.bucket,
											datastore: vals.datastore,
											namespace: vals.ns || "",
											backup_type: vals["backup-type"],
											backup_id: vals["backup-id"],
										}],
										credentials: [{
											access_key: vals["access-key"],
											secret_key: vals["secret-key"],
											auth_id: vals["auth-id"],
											grants: [{
												bucket: vals.bucket,
												read: vals.read === "1",
												write: vals.write === "1",
												delete: vals.delete === "1",
											}],
										}],
									};
									if (vals["tls-cert"]) s3["tls-cert"] = vals["tls-cert"];
									if (vals["tls-key"]) s3["tls-key"] = vals["tls-key"];
									if (vals["spool-dir"]) s3["spool-dir"] = vals["spool-dir"];
									params.s3 = JSON.stringify(s3);
								}
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
			if (complexS3 && values.s3) {
				win.down("form").down("[name=s3]").setValue(JSON.stringify(values.s3, null, 2));
			}
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
			if (v === "s3") return "S3";
			return Ext.String.htmlEncode(v || "");
		`)},
		{Text: "Listen Address", DataIndex: "listen-addr", Width: 160, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Access", DataIndex: "valid-users", Width: 180, Renderer: js.Func("v, meta, rec", `
			if (rec.get("type") === "s3") {
				let s3 = rec.get("s3");
				return (s3 && s3.buckets ? s3.buckets.length : 0) + " bucket(s)";
			}
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
