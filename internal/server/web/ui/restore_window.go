package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

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
	Subject: "Disk Restore Job", IsAdd: true,
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
				let pathSel = this.lookup("pathSelectorDestination");
				if (pathSel) {
					pathSel.setTarget(value);
				}
			`),
		},
	},
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			let me = this;
			me.callParent();
			if (me.jobData) {
				let data = Ext.apply({}, me.jobData);
				me.setValues(data);
			}
		`),
	},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Panel{
				Extend: js.ExtInputPanel, Title: "Options",
				CBind: js.Obj{"isCreate": "{isCreate}"},
				Methods: map[string]js.Raw{
					"onGetValues": js.Func("values", `
						let me = this;
						if (me.isCreate) {
							delete values.delete;
						}
						return values;
					`),
				},
				Column1: js.Items(
					js.Field{XType: js.XDisplayEditField, Name: "id", Label: "Job ID", Renderer: "Ext.htmlEncode", AllowBlank: new(true), EditableWhenCreate: true},
					js.Field{XType: js.XCombo, Label: "Restore Mode", Name: "mode", QueryMode: "local", Store: js.Raw("restoreModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true,
						AllowBlank: new(true), CBind: js.Obj{"value": "{restoreModeValue}"}},
					js.Field{XType: js.XDataStoreSelector, Label: "Local Datastore", Name: "store"},
					js.Field{XType: "pbsD2DNamespaceSelector", Label: "Snapshot namespace", EmptyText: "Root", Name: "ns", Reference: "namespace", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: "pbsD2DSnapshotSelector", Label: "Snapshot", Name: "snapshot", Reference: "snapshot", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: "pbsD2DSnapshotPathSelector", Label: "Path to restore", Reference: "pathSelector", Name: "src-path", DeleteEmptyWhenNotCreate: true},
				),
				Column2: js.Items(
					js.Field{XType: "pbsD2DTargetSelector", Label: "Target restore destination", Name: "dest-target", Reference: "dest-target"},
					js.Field{XType: "pbsD2DTargetPathSelector", Label: "Path to destination", Reference: "pathSelectorDestination", Name: "dest-subpath", OnlyDirs: true, DeleteEmptyWhenNotCreate: true},
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
