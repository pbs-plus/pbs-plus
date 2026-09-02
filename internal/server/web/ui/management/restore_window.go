package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var restoreModesStore = js.Raw(`
var restoreModes = Ext.create("Ext.data.Store", {
	fields: ["display", "value"],
	data: [
		{ display: "Normal", value: "0" },
		{ display: "Zipped", value: "1" },
		{ display: "No Attributes", value: "2" },
	],
});
`)

//go:fix inline
var restoreJobEdit = js.EditWindow{
	Name: "PBS.D2DManagement.RestoreJobEdit", XType: "pbsDiskRestoreJobEdit",
	Subject: "Restore Job", IsAdd: true,
	FieldDefaults: js.Obj{"labelWidth": 120},
	BodyPadding:   new(0),
	CBindData: js.Func("initialConfig", `
		let me = this;
		let baseurl = "/api2/extjs/config/disk-restore";
		let id = initialConfig.id;
		me.isCreate = !id;
		me.url = id ? baseurl + "/" + encodeURIComponent(encodePathValue(id)) : baseurl;
		me.method = id ? "PUT" : "POST";
		me.autoLoad = !!id;
		me.authid = id ? null : Proxmox.UserName;
		me.restoreModeValue = id ? null : "0";
		me.editDatastore = me.datastore === undefined && me.isCreate;
		return {};
	`),
	ViewModelData: js.Obj{},
	Controller: js.Controller{
		Control: js.Obj{
			"pbsDataStoreSelector[name=store]":       js.Obj{"change": "storeChange"},
			"pbsD2DNamespaceSelector[name=ns]":       js.Obj{"change": "nsChange"},
			"pbsD2DSnapshotSelector[name=snapshot]":  js.Obj{"change": "snapshotChange"},
			"pbsD2DTargetSelector[name=dest-target]": js.Obj{"change": "targetChange"},
		},
		Methods: map[string]js.Raw{
			"storeChange": js.Func("field, value", `
				let me = this;
				let nsSelector = me.lookup("namespace");
				let snapSelector = me.lookup("snapshot");
				nsSelector.setDatastore(value);
				snapSelector.setDatastore(value);
				let pathSel = me.lookup("pathSelector");
				if (pathSel) pathSel.setDatastore(value);
				if (field.isDirty()) {
					snapSelector.setValue(null);
				}
			`),
			"nsChange": js.Func("field, value", `
				let me = this;
				let snapSelector = me.lookup("snapshot");
				snapSelector.setNamespace(value);
				let pathSel = me.lookup("pathSelector");
				if (pathSel) pathSel.setNamespace(value);
				if (field.isDirty()) {
					snapSelector.setValue(null);
				}
			`),
			"snapshotChange": js.Func("field, value", `
				let pathSel = this.lookup("pathSelector");
				if (pathSel) {
					pathSel.setSnapshot(value);
				}
			`),
			"targetChange": js.Func("field, value", `
				let record = field.getStore().findRecord("name", value, 0, false, true, true);
				let kind = record ? (record.get("kind") || record.get("target_type")) : "filesystem";
				let database = ["postgresql", "mysql", "ldap"].includes(kind);
				let ldap = kind === "ldap";
				let sourcePath = this.lookup("pathSelector");
				sourcePath.setHidden(database);
				sourcePath.setDisabled(database);
				let names = [];
				field.getStore().each(function (entry) {
					let entryKind = entry.get("kind") || entry.get("target_type");
					let keep = database ? entryKind === kind : ["postgresql", "mysql", "ldap"].includes(entryKind);
					if (keep) {
						names.push(String(entry.get("name") || "").replace(/[. ]/g, "-"));
					}
				});
				this.lookup("snapshot").setArchiveFilter({ names: names, mode: database ? "include" : "exclude" });
				let pathSel = this.lookup("pathSelectorDestination");
				if (pathSel) {
					pathSel.setTarget(value);
				}
				let filesystemDestination = this.lookup("filesystemDestination");
				filesystemDestination.setHidden(database);
				filesystemDestination.setDisabled(database);
				let databaseDestination = this.lookup("databaseDestination");
				databaseDestination.setHidden(!database);
				databaseDestination.setDisabled(!database);
				let sourceDatabase = this.lookup("sourceDatabase");
				let destinationDatabase = this.lookup("destinationDatabase");
				if (sourceDatabase) {
					sourceDatabase.setFieldLabel(ldap ? "Source DN" : "Source Database");
					sourceDatabase.setEmptyText(ldap ? "Entire base DN" : "Entire server");
				}
				if (destinationDatabase) {
					destinationDatabase.setHidden(ldap);
					destinationDatabase.setDisabled(ldap);
				}
				let restoreMode = this.lookup("filesystemRestoreMode");
				restoreMode.setHidden(database);
				restoreMode.setDisabled(database);
			`),
		},
	},
	Methods: map[string]js.Raw{"initComponent": js.ApplyJobData},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Panel{
				Extend: js.ExtInputPanel, Title: "Options",
				CBind:   js.Obj{"isCreate": "{isCreate}"},
				Methods: map[string]js.Raw{"onGetValues": js.DropDeleteOnCreate},
				Column1: js.Items(
					js.Field{XType: js.XDisplayEditField, Name: "id", Label: "Job ID", Renderer: "Ext.htmlEncode", AllowBlank: new(true), EditableWhenCreate: true},
					js.Field{XType: js.XCombo, Label: "Restore Mode", Name: "mode", Reference: "filesystemRestoreMode", QueryMode: "local", Store: js.Raw("restoreModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true,
						AllowBlank: new(true), CBind: js.Obj{"value": "{restoreModeValue}"}},
					js.Field{XType: js.XDataStoreSelector, Label: "Local Datastore", Name: "store"},
					js.Field{XType: "pbsD2DNamespaceSelector", Label: "Snapshot namespace", EmptyText: "Root", Name: "ns", Reference: "namespace", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: "pbsD2DSnapshotSelector", Label: "Snapshot", Name: "snapshot", Reference: "snapshot", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: "pbsD2DSnapshotPathSelector", Label: "Path to restore", Reference: "pathSelector", Name: "src-path", DeleteEmptyWhenNotCreate: true},
				),
				Column2: js.Items(
					js.Field{XType: "pbsD2DTargetSelector", Label: "Target restore destination", Name: "dest-target", Reference: "dest-target"},
					js.Field{XType: js.XFieldContainer, Reference: "filesystemDestination", Layout: "anchor", Items: js.Items(
						js.Field{XType: "pbsD2DTargetPathSelector", Label: "Path to destination", Reference: "pathSelectorDestination", Name: "dest-subpath", OnlyDirs: true, DeleteEmptyWhenNotCreate: true},
					)},
					js.Field{XType: js.XFieldContainer, Reference: "databaseDestination", Layout: "anchor", Hidden: true, Disabled: true, Items: js.Items(
						js.Field{XType: "proxmoxtextfield", Label: "Source Database", Name: "source_database", Reference: "sourceDatabase", AllowBlank: new(true), EmptyText: "Entire server", DeleteEmptyWhenNotCreate: true,
							AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Database to take out of an entire-server snapshot. Leave empty to restore every database in the snapshot.")}},
						js.Field{XType: "proxmoxtextfield", Label: "Destination Database", Name: "destination_database", Reference: "destinationDatabase", AllowBlank: new(true), EmptyText: "Same as source", DeleteEmptyWhenNotCreate: true,
							AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Name to restore the database under. Leave empty to keep the name it had in the snapshot.")}},
						js.Field{XType: js.XCheckbox, Label: "Replace Existing", Name: "replace_existing", BoxLabel: "Delete and recreate the selected database or LDAP subtree", InputValue: "true", UncheckedValue: "false"},
					)},
				),
				ColumnB: js.Items(
					js.Field{XType: "proxmoxtextfield", Label: "Comment", Name: "comment", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: "pbsD2DScriptSelector", Label: "Pre-Restore Script", Name: "pre_script"},
					js.Field{XType: "pbsD2DScriptSelector", Label: "Post-Restore Script", Name: "post_script"},
				),
			},
			js.Raw("PBS.D2DManagement.makeNotificationTab()"),
		),
	}),
}
