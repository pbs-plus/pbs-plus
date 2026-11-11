// Package s3repo defines the S3 backup repository configuration panel.
package s3repo

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func Definitions() []js.Value {
	return []js.Value{repositoriesModel, repositoriesPanel}
}

var repositoriesModel = js.Model{
	Name:       "pbs-model-s3-repositories",
	Fields:     js.Fields("name", "listen-addr", "config", "running", "error", "endpoints"),
	IDProperty: "name",
}

var repositoriesPanel = js.Panel{
	Name: "PBS.D2DS3Repository.RepositoriesPanel", XType: "pbsPlusS3RepositoriesPanel",
	Title:     "S3 Backup Repositories",
	Store:     js.Store{StoreID: "pbs-plus-s3-repositories", Model: "pbs-model-s3-repositories", Interval: 5000, APIPath: "/api2/extjs/config/d2d-s3-repositories", Sorters: "name"},
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
			let rec = this.getView().getSelectionModel().getSelection()[0];
			if (!rec) {
				Ext.Msg.alert(gettext("Error"), gettext("Please select a repository."));
				return;
			}
			this.openEdit(rec);
		`),
		"openEdit": js.Func("rec", `
			let isEdit = !!rec;
			let values = isEdit ? rec.data : {};
			let config = Ext.clone(values.config || {});
			let panel = this.getView();
			let bucketStore = Ext.create("Ext.data.Store", {
				fields: ["name", "datastore", "namespace", "backup_type", "backup_id"],
				data: config.buckets || [],
			});
			let credentialStore = Ext.create("Ext.data.Store", {
				fields: ["access_key", "secret_key", "auth_id", "grants"],
				data: config.credentials || [],
			});
			let win;
			let openBucketEditor = (record) => {
				let current = record ? record.data : {};
				let editor = Ext.create("PBS.plusWindow.Window", {
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
								xtype: "pbsDataStoreSelector",
								name: "bucket-datastore",
								fieldLabel: gettext("Datastore"),
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
							win.down("[itemId=credentialGrid]").getView().refresh();
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
				let editor = Ext.create("PBS.plusWindow.Window", {
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
							for (let row of editor.down("fieldset").query("fieldcontainer")) {
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
			let selectedRecord = (itemId) => win.down("[itemId=" + itemId + "]").getSelectionModel().getSelection()[0];
			win = Ext.create("PBS.plusWindow.Window", {
				title: isEdit ? Ext.String.format(gettext("Edit S3 Backup Repository '{0}'"), values.name) : gettext("Add S3 Backup Repository"),
				width: 860,
				height: 720,
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
							xtype: "displayfield",
							value: gettext("An S3 endpoint for backup tools such as mariadb-operator, Velero or pgBackRest. Every uploaded object becomes a snapshot in the mapped backup group; clients authenticate with SigV4 access keys."),
						},
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
							xtype: "fieldset",
							title: gettext("Connection"),
							items: [
								{
									xtype: "proxmoxtextfield",
									name: "listen-addr",
									fieldLabel: gettext("Listen Address"),
									emptyText: "0.0.0.0:9000",
									allowBlank: false,
									value: values["listen-addr"] || "0.0.0.0:9000",
								},
								{
									xtype: "combobox",
									name: "region",
									fieldLabel: gettext("Region"),
									store: ["us-east-1", "us-east-2", "us-west-1", "us-west-2", "eu-central-1", "eu-west-1", "eu-west-2", "ap-southeast-1", "ap-northeast-1", "sa-east-1"],
									queryMode: "local",
									editable: true,
									forceSelection: false,
									value: config.region || "us-east-1",
								},
								{
									xtype: "proxmoxcheckbox",
									name: "tls",
									fieldLabel: gettext("HTTPS"),
									boxLabel: gettext("Serve HTTPS using the current PBS certificate"),
									inputValue: "1",
									uncheckedValue: "0",
									checked: config.tls !== false,
									listeners: {
										change: (field, enabled) => {
											let form = field.up("form");
											for (let name of ["tls-cert", "tls-key"]) form.down("[name=" + name + "]").setDisabled(!enabled);
										},
									},
								},
								{
									xtype: "proxmoxtextfield",
									name: "tls-cert",
									fieldLabel: gettext("Custom Certificate"),
									emptyText: gettext("Current PBS certificate (default)"),
									value: config["tls-cert"],
									disabled: config.tls === false,
								},
								{
									xtype: "proxmoxtextfield",
									name: "tls-key",
									fieldLabel: gettext("Custom Key"),
									emptyText: gettext("Current PBS key (default)"),
									value: config["tls-key"],
									disabled: config.tls === false,
								},
								{
									xtype: "proxmoxtextfield",
									name: "spool-dir",
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
									win.down("[itemId=credentialGrid]").getView().refresh();
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
					text: isEdit ? gettext("Save") : gettext("Create"),
					handler: () => {
						let form = win.down("form").getForm();
						if (!form.isValid()) return;
						if (!bucketStore.getCount()) {
							Ext.Msg.alert(gettext("Error"), gettext("Add at least one bucket."));
							return;
						}
						if (!credentialStore.getCount()) {
							Ext.Msg.alert(gettext("Error"), gettext("Add at least one credential."));
							return;
						}
						if (credentialStore.findBy((credential) => !(credential.get("grants") || []).length) !== -1) {
							Ext.Msg.alert(gettext("Error"), gettext("Every credential needs at least one bucket permission."));
							return;
						}
						let vals = form.getValues();
						let tlsEnabled = vals.tls === "1";
						let cert = vals["tls-cert"] || "";
						let key = vals["tls-key"] || "";
						if (tlsEnabled && (!!cert !== !!key)) {
							Ext.Msg.alert(gettext("Error"), gettext("Set both the custom TLS certificate and key, or leave both empty."));
							return;
						}
						let next = {
							region: vals.region || "us-east-1",
							tls: tlsEnabled,
							buckets: bucketStore.getRange().map((row) => Ext.clone(row.data)),
							credentials: credentialStore.getRange().map((row) => Ext.clone(row.data)),
						};
						if (cert) next["tls-cert"] = cert;
						if (key) next["tls-key"] = key;
						if (vals["spool-dir"]) next["spool-dir"] = vals["spool-dir"];
						let url = "/api2/extjs/config/d2d-s3-repositories";
						let method = "POST";
						if (isEdit) {
							method = "PUT";
							url += "/" + encodeURIComponent(values.name);
						}
						PBS.PlusUtils.API2Request({
							url,
							method,
							params: { name: vals.name || values.name, "listen-addr": vals["listen-addr"], config: JSON.stringify(next) },
							waitMsgTarget: win,
							failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
							success: () => {
								win.close();
								panel.getStore().rstore.load();
							},
						});
					},
				}],
			});
			win.show();
		`),
		"edit": js.Func("view, rowIdx, colIdx, item, e, rec", `
			this.openEdit(rec);
		`),
		"remove": js.Func("view, rowIdx, colIdx, item, e, rec", `
			let panel = this.getView();
			Ext.Msg.confirm(
				gettext("Remove S3 Backup Repository"),
				Ext.String.format(gettext("Remove repository {0}? Snapshots already written stay in their datastores."), rec.data.name),
				(btn) => {
					if (btn !== "yes") return;
					PBS.PlusUtils.API2Request({
						url: "/api2/extjs/config/d2d-s3-repositories/" + encodeURIComponent(rec.data.name),
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
		{Text: "Listen Address", DataIndex: "listen-addr", Width: 160, Renderer: js.Func("v", `return Ext.String.htmlEncode(v || "");`)},
		{Text: "Access", DataIndex: "config", Width: 200, Renderer: js.Func("v", `
			let config = v || {};
			return Ext.String.format(gettext("{0} bucket(s), {1} credential(s)"), (config.buckets || []).length, (config.credentials || []).length);
		`)},
		{Text: "Status", DataIndex: "running", Width: 100, Renderer: js.Func("v, meta, rec", `
			if (v) return '<i class="fa fa-check-circle"></i> ' + gettext("Running");
			let error = rec.get("error") || "";
			if (error) meta.tdAttr = 'data-qtip="' + Ext.String.htmlEncode(error) + '"';
			return '<i class="fa fa-times-circle"></i> ' + gettext("Stopped");
		`)},
		{Text: "Endpoints", DataIndex: "endpoints", Flex: 1, Renderer: js.Func("v", `
			return Ext.String.htmlEncode((v || []).join(", ") || "-");
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
