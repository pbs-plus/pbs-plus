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
			let managedS3 = values.s3 ? Ext.clone(values.s3) : null;
			let win;
			let updateS3Summary = () => {
				if (!win) return;
				let summary = win.down("[itemId=s3Summary]");
				if (!summary) return;
				let config = managedS3 || {};
				let buckets = config.buckets ? config.buckets.length : 0;
				let credentials = config.credentials ? config.credentials.length : 0;
				summary.setValue(Ext.String.format(gettext("{0} bucket(s), {1} credential(s)"), buckets, credentials));
			};
			let openS3Manager = () => {
				let config = Ext.clone(managedS3 || {});
				let bucketStore = Ext.create("Ext.data.Store", {
					fields: ["name", "datastore", "namespace", "backup_type", "backup_id"],
					data: config.buckets || [],
				});
				let credentialStore = Ext.create("Ext.data.Store", {
					fields: ["access_key", "secret_key", "auth_id", "grants"],
					data: config.credentials || [],
				});
				let manager;
				let openBucketEditor = (record) => {
					let current = record ? record.data : {};
					let editor = Ext.create("Ext.window.Window", {
						title: record ? gettext("Edit Bucket Mapping") : gettext("Add Bucket Mapping"),
						width: 500,
						modal: true,
						bodyPadding: 10,
						items: [{
							xtype: "form",
							border: false,
							defaults: { anchor: "100%", labelWidth: 120 },
							items: [
								{
									xtype: "proxmoxtextfield",
									name: "bucket-name",
									fieldLabel: gettext("Bucket Name"),
									allowBlank: false,
									regex: /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/,
									regexText: gettext("3-63 lowercase letters, digits, dots and dashes"),
									value: current.name,
								},
								{
									xtype: "combobox",
									name: "bucket-datastore",
									fieldLabel: gettext("Datastore"),
									store: "pbs-datastore-list",
									displayField: "store",
									valueField: "store",
									allowBlank: false,
									value: current.datastore,
									listeners: {
										change: (cb, value) => {
											let selector = cb.up("form").down("pbsNamespaceSelector");
											if (selector) selector.setDatastore(value);
										},
									},
								},
								{
									xtype: "pbsNamespaceSelector",
									name: "bucket-namespace",
									fieldLabel: gettext("Namespace"),
									datastore: current.datastore,
									emptyText: gettext("root"),
									value: current.namespace,
								},
								{
									xtype: "combobox",
									name: "bucket-backup-type",
									fieldLabel: gettext("Backup Type"),
									store: [["host", gettext("Host")], ["vm", gettext("VM")], ["ct", gettext("Container")]],
									value: current.backup_type || "host",
									editable: false,
									allowBlank: false,
								},
								{
									xtype: "proxmoxtextfield",
									name: "bucket-backup-id",
									fieldLabel: gettext("Backup ID"),
									allowBlank: false,
									regex: /^[A-Za-z0-9_][A-Za-z0-9._-]*$/,
									regexText: gettext("Letters, digits, dots, dashes and underscores; start with a letter or digit"),
									value: current.backup_id,
								},
							],
						}],
						buttons: [{
							text: record ? gettext("Apply") : gettext("Add"),
							handler: () => {
								let form = editor.down("form").getForm();
								if (!form.isValid()) return;
								let vals = form.getValues();
								let name = vals["bucket-name"];
								let duplicate = bucketStore.findExact("name", name);
								if (duplicate !== -1 && (!record || bucketStore.getAt(duplicate) !== record)) {
									Ext.Msg.alert(gettext("Error"), gettext("Bucket names must be unique."));
									return;
								}
								let data = {
									name,
									datastore: vals["bucket-datastore"],
									namespace: vals["bucket-namespace"] || "",
									backup_type: vals["bucket-backup-type"],
									backup_id: vals["bucket-backup-id"],
								};
								if (record) {
									let oldName = record.get("name");
									record.set(data);
									if (oldName !== name) {
										credentialStore.each((credential) => {
											let grants = (credential.get("grants") || []).map((grant) => grant.bucket === oldName ? Ext.apply({}, grant, { bucket: name }) : grant);
											credential.set("grants", grants);
										});
									}
								} else {
									bucketStore.add(data);
								}
								manager.down("[itemId=credentialGrid]").getView().refresh();
								editor.close();
							},
						}],
					});
					editor.show();
				};
				let openCredentialEditor = (record) => {
					if (!bucketStore.getCount()) {
						Ext.Msg.alert(gettext("Add a Bucket"), gettext("Add at least one bucket before creating credentials."));
						return;
					}
					let current = record ? record.data : {};
					let grantMap = {};
					for (let grant of current.grants || []) grantMap[grant.bucket] = grant;
					let grantRows = bucketStore.getRange().map((bucketRecord) => {
						let bucket = bucketRecord.get("name");
						let grant = grantMap[bucket] || {};
						return {
							xtype: "fieldcontainer",
							fieldLabel: bucket,
							layout: "hbox",
							bucketName: bucket,
							defaults: { xtype: "checkbox", margin: "0 18 0 0" },
							items: [
								{ itemId: "read", boxLabel: gettext("Read"), checked: !!grant.read },
								{ itemId: "write", boxLabel: gettext("Write"), checked: !!grant.write },
								{ itemId: "delete", boxLabel: gettext("Delete"), checked: !!grant.delete },
							],
						};
					});
					let editor = Ext.create("Ext.window.Window", {
						title: record ? gettext("Edit Credential") : gettext("Add Credential"),
						width: 540,
						maxHeight: 650,
						modal: true,
						bodyPadding: 10,
						items: [{
							xtype: "form",
							border: false,
							autoScroll: true,
							defaults: { anchor: "100%", labelWidth: 150 },
							items: [
								{
									xtype: "proxmoxtextfield",
									name: "credential-access-key",
									fieldLabel: gettext("Access Key"),
									allowBlank: false,
									value: current.access_key,
								},
								{
									xtype: "proxmoxtextfield",
									name: "credential-secret-key",
									fieldLabel: gettext("Secret Key"),
									inputType: "password",
									allowBlank: !!record,
									minLength: 8,
									emptyText: record ? gettext("Unchanged") : "",
								},
								{
									xtype: "proxmoxtextfield",
									name: "credential-auth-id",
									fieldLabel: gettext("PBS Owner"),
									allowBlank: false,
									regex: /^[A-Za-z0-9._-]+@[A-Za-z0-9._-]+(![A-Za-z0-9._-]+)?$/,
									regexText: gettext("PBS auth id, e.g. root@pam"),
									value: current.auth_id || "root@pam",
								},
								{
									xtype: "fieldset",
									title: gettext("Bucket Permissions"),
									items: grantRows,
								},
							],
						}],
						buttons: [{
							text: record ? gettext("Apply") : gettext("Add"),
							handler: () => {
								let form = editor.down("form").getForm();
								if (!form.isValid()) return;
								let vals = form.getValues();
								let accessKey = vals["credential-access-key"];
								let duplicate = credentialStore.findExact("access_key", accessKey);
								if (duplicate !== -1 && (!record || credentialStore.getAt(duplicate) !== record)) {
									Ext.Msg.alert(gettext("Error"), gettext("Access keys must be unique."));
									return;
								}
								let secretKey = vals["credential-secret-key"] || current.secret_key || "";
								if (!secretKey && (!record || accessKey !== current.access_key)) {
									Ext.Msg.alert(gettext("Error"), gettext("A secret key is required for a new access key."));
									return;
								}
								let grants = [];
								let rows = editor.down("fieldset").query("fieldcontainer");
								for (let row of rows) {
									let read = row.down("[itemId=read]").getValue();
									let write = row.down("[itemId=write]").getValue();
									let remove = row.down("[itemId=delete]").getValue();
									if (read || write || remove) grants.push({ bucket: row.bucketName, read, write, delete: remove });
								}
								if (!grants.length) {
									Ext.Msg.alert(gettext("Error"), gettext("Assign at least one bucket permission."));
									return;
								}
								let data = { access_key: accessKey, secret_key: secretKey, auth_id: vals["credential-auth-id"], grants };
								if (record) record.set(data); else credentialStore.add(data);
								editor.close();
							},
						}],
					});
					editor.show();
				};
				let selectedRecord = (itemId) => manager.down("[itemId=" + itemId + "]").getSelectionModel().getSelection()[0];
				manager = Ext.create("Ext.window.Window", {
					title: gettext("S3 Buckets and Credentials"),
					width: 860,
					height: 650,
					modal: true,
					layout: "fit",
					items: [{
						xtype: "form",
						border: false,
						autoScroll: true,
						bodyPadding: 10,
						defaults: { anchor: "100%", labelWidth: 150 },
						items: [
							{
								xtype: "fieldset",
								title: gettext("Connection"),
								items: [
									{
										xtype: "combobox",
										name: "manager-region",
										fieldLabel: gettext("Region"),
										store: ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-central-1", "eu-west-1", "eu-west-2", "ap-southeast-1", "ap-northeast-1", "sa-east-1"],
										queryMode: "local",
										editable: true,
										forceSelection: false,
										value: config.region || "us-east-1",
									},
									{
										xtype: "proxmoxcheckbox",
										name: "manager-tls",
										fieldLabel: gettext("HTTPS"),
										boxLabel: gettext("Serve HTTPS using the current PBS certificate"),
										inputValue: "1",
										uncheckedValue: "0",
										checked: config.tls !== false,
										listeners: {
											change: (field, enabled) => {
												let form = field.up("form");
												for (let name of ["manager-tls-cert", "manager-tls-key"]) form.down("[name=" + name + "]").setDisabled(!enabled);
											},
										},
									},
									{
										xtype: "proxmoxtextfield",
										name: "manager-tls-cert",
										fieldLabel: gettext("Custom Certificate"),
										emptyText: gettext("Current PBS certificate (default)"),
										value: config["tls-cert"],
										disabled: config.tls === false,
									},
									{
										xtype: "proxmoxtextfield",
										name: "manager-tls-key",
										fieldLabel: gettext("Custom Key"),
										emptyText: gettext("Current PBS key (default)"),
										value: config["tls-key"],
										disabled: config.tls === false,
									},
									{
										xtype: "proxmoxtextfield",
										name: "manager-spool-dir",
										fieldLabel: gettext("Spool Directory"),
										emptyText: gettext("inside the datastore (default)"),
										value: config["spool-dir"],
									},
								],
							},
							{
								xtype: "grid",
								itemId: "bucketGrid",
								title: gettext("Bucket Mappings"),
								height: 180,
								store: bucketStore,
								columns: [
									{ text: gettext("Bucket"), dataIndex: "name", flex: 1 },
									{ text: gettext("Datastore"), dataIndex: "datastore", flex: 1 },
									{ text: gettext("Namespace"), dataIndex: "namespace", flex: 1, renderer: (value) => Ext.String.htmlEncode(value || gettext("root")) },
									{ text: gettext("Backup Group"), flex: 1, renderer: (value, meta, row) => Ext.String.htmlEncode(row.get("backup_type") + "/" + row.get("backup_id")) },
								],
								tbar: [
									{ text: gettext("Add"), iconCls: "fa fa-plus", handler: () => openBucketEditor(null) },
									{ text: gettext("Edit"), iconCls: "fa fa-pencil", handler: () => { let selected = selectedRecord("bucketGrid"); if (selected) openBucketEditor(selected); } },
									{ text: gettext("Remove"), iconCls: "fa fa-trash", handler: () => {
										let selected = selectedRecord("bucketGrid");
										if (!selected) return;
										let name = selected.get("name");
										bucketStore.remove(selected);
										credentialStore.each((credential) => credential.set("grants", (credential.get("grants") || []).filter((grant) => grant.bucket !== name)));
										manager.down("[itemId=credentialGrid]").getView().refresh();
									} },
								],
								listeners: { itemdblclick: (grid, selected) => openBucketEditor(selected) },
							},
							{
								xtype: "grid",
								itemId: "credentialGrid",
								title: gettext("Credentials"),
								height: 180,
								margin: "10 0 0 0",
								store: credentialStore,
								columns: [
									{ text: gettext("Access Key"), dataIndex: "access_key", flex: 1 },
									{ text: gettext("PBS Owner"), dataIndex: "auth_id", flex: 1 },
									{ text: gettext("Bucket Access"), dataIndex: "grants", flex: 2, renderer: (grants) => Ext.String.htmlEncode((grants || []).map((grant) => grant.bucket).join(", ") || gettext("None")) },
								],
								tbar: [
									{ text: gettext("Add"), iconCls: "fa fa-plus", handler: () => openCredentialEditor(null) },
									{ text: gettext("Edit"), iconCls: "fa fa-pencil", handler: () => { let selected = selectedRecord("credentialGrid"); if (selected) openCredentialEditor(selected); } },
									{ text: gettext("Remove"), iconCls: "fa fa-trash", handler: () => { let selected = selectedRecord("credentialGrid"); if (selected) credentialStore.remove(selected); } },
								],
								listeners: { itemdblclick: (grid, selected) => openCredentialEditor(selected) },
							},
						],
					}],
					buttons: [{
						text: gettext("Apply"),
						handler: () => {
							if (!bucketStore.getCount()) {
								Ext.Msg.alert(gettext("Error"), gettext("Add at least one bucket."));
								return;
							}
							if (!credentialStore.getCount()) {
								Ext.Msg.alert(gettext("Error"), gettext("Add at least one credential."));
								return;
							}
							let missingGrant = credentialStore.findBy((credential) => !(credential.get("grants") || []).length);
							if (missingGrant !== -1) {
								Ext.Msg.alert(gettext("Error"), gettext("Every credential needs at least one bucket permission."));
								return;
							}
							let vals = manager.down("form").getForm().getValues();
							let tlsEnabled = vals["manager-tls"] === "1";
							let cert = vals["manager-tls-cert"] || "";
							let key = vals["manager-tls-key"] || "";
							if (tlsEnabled && (!!cert !== !!key)) {
								Ext.Msg.alert(gettext("Error"), gettext("Set both the custom TLS certificate and key, or leave both empty."));
								return;
							}
							managedS3 = {
								region: vals["manager-region"] || "us-east-1",
								tls: tlsEnabled,
								buckets: bucketStore.getRange().map((row) => Ext.clone(row.data)),
								credentials: credentialStore.getRange().map((row) => Ext.clone(row.data)),
							};
							if (cert) managedS3["tls-cert"] = cert;
							if (key) managedS3["tls-key"] = key;
							if (vals["manager-spool-dir"]) managedS3["spool-dir"] = vals["manager-spool-dir"];
							updateS3Summary();
							manager.close();
						},
					}],
				});
				manager.show();
			};
			win = Ext.create("Ext.window.Window", {
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
									if (!isEdit && listen) {
										if (v === "s3" && listen.getValue() === "0.0.0.0:2049") listen.setValue("0.0.0.0:9000");
										if (v === "nfs" && listen.getValue() === "0.0.0.0:9000") listen.setValue("0.0.0.0:2049");
									}
									let smb = form.down("[itemId=sambaFields]");
									let structured = form.down("[itemId=s3Fields]");
									if (listen) {
										listen.setVisible(v !== "samba");
										listen.setDisabled(v === "samba");
									}
									if (smb) {
										smb.setVisible(v === "samba");
										smb.setDisabled(v !== "samba");
									}
									if (structured) {
										structured.setVisible(v === "s3");
										structured.setDisabled(v !== "s3");
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
							value: values["listen-addr"] || (values.type === "s3" ? "0.0.0.0:9000" : "0.0.0.0:2049"),
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
							itemId: "s3Fields",
							defaults: { anchor: "100%", labelWidth: 120 },
							hidden: values.type !== "s3",
							disabled: values.type !== "s3",
							items: [
								{
									xtype: "displayfield",
									itemId: "s3Summary",
									fieldLabel: gettext("S3 Configuration"),
								},
								{
									xtype: "button",
									text: gettext("Manage Buckets and Credentials"),
									iconCls: "fa fa-list",
									margin: "0 0 10 0",
									handler: openS3Manager,
								},
								{
									xtype: "displayfield",
									value: gettext("Objects become snapshots in the mapped backup group; clients authenticate with SigV4 access keys."),
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
								if (!managedS3 || !(managedS3.buckets || []).length || !(managedS3.credentials || []).length) {
									Ext.Msg.alert(gettext("Error"), gettext("Use 'Manage Buckets and Credentials' to add at least one bucket and one credential."));
									return;
								}
								params.s3 = JSON.stringify(managedS3);
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
			updateS3Summary();
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
				let s3 = rec.get("s3") || {};
				return Ext.String.format(gettext("{0} bucket(s), {1} credential(s)"), (s3.buckets || []).length, (s3.credentials || []).length);
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
			if (rec.get("type") === "s3") return Ext.String.htmlEncode(endpoints.join(", ") || "-");
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
