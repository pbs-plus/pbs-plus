package management

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func modeStore(varName string, options ...[2]string) js.Raw {
	var b strings.Builder
	b.WriteString("var " + varName + " = Ext.create(\"Ext.data.Store\", {\n")
	b.WriteString("\tfields: [\"display\", \"value\"],\n\tdata: [\n")
	for _, o := range options {
		fmt.Fprintf(&b, "\t\t{ display: %s, value: %s },\n", js.Raw(strconv.Quote(o[0])), js.Raw(strconv.Quote(o[1])))
	}
	b.WriteString("\t],\n});\n")
	return js.Raw(b.String())
}

var backupModeStores = js.Raw(strings.Join([]string{
	string(modeStore("backupModes",
		[2]string{"Metadata", "metadata"}, [2]string{"Data", "data"}, [2]string{"Legacy", "legacy"})),
	string(modeStore("sourceModes", [2]string{"Snapshot", "snapshot"}, [2]string{"Direct", "direct"})),
	string(modeStore("readModes", [2]string{"Standard", "standard"})),
	string(modeStore("xattrModes",
		[2]string{"Include extra security attributes", "true"}, [2]string{"Exclude extra security attributes", "false"})),
	string(modeStore("legacyXattrModes",
		[2]string{"Use broken xattr", "true"}, [2]string{"Use fixed xattr", "false"})),
}, ""))

var backupJobEdit = js.EditWindow{
	Name: "PBS.D2DManagement.BackupJobEdit", XType: "pbsDiskBackupJobEdit",
	Subject: "Disk Backup Job", IsAdd: true,
	FieldDefaults: js.Obj{"labelWidth": 120},
	BodyPadding:   new(0),
	CBindData: js.Func("initialConfig", `
		let me = this;
		let baseurl = "/api2/extjs/config/disk-backup";
		let id = initialConfig.id;
		me.isCreate = !id;
		me.url = id ? baseurl + "/" + encodeURIComponent(encodePathValue(id)) : baseurl;
		me.method = id ? "PUT" : "POST";
		me.autoLoad = !!id;
		me.scheduleValue = id ? null : "";
		me.backupModeValue = id ? null : "metadata";
		me.sourceModeValue = id ? null : "snapshot";
		me.readModeValue = id ? null : "standard";
		me.includeXAttrValue = id ? null : "true";
		me.legacyXAttrValue = id ? null : "false";
		me.authid = id ? null : Proxmox.UserName;
		me.editDatastore = me.datastore === undefined && me.isCreate;
		return {};
	`),
	ViewModelData: js.Obj{},
	Controller: js.Controller{
		Control: js.Obj{
			"pbsDataStoreSelector[name=store]":  js.Obj{"change": "storeChange"},
			"pbsD2DTargetSelector[name=target]": js.Obj{"change": "targetChange"},
		},
		Methods: map[string]js.Raw{
			"storeChange": js.Func("field, value", `
				let nsSelector = this.lookup("namespace");
				nsSelector.setDatastore(value);
			`),
			"targetChange": js.Func("field, value", `
				let pathSel = this.lookup("pathSelectorSubpath");
				if (pathSel) {
					pathSel.setTarget(value);
				}
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
					js.Field{XType: "pbsD2DTargetSelector", Label: "Target", Name: "target", Reference: "target"},
					js.Field{XType: "pbsD2DTargetPathSelector", Label: "Subpath", Reference: "pathSelectorSubpath", Name: "subpath", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: js.XDataStoreSelector, Label: "Local Datastore", Name: "store"},
					js.Field{XType: "pbsD2DNamespaceSelector", Label: "Namespace", EmptyText: "Root", Name: "ns", Reference: "namespace", DeleteEmptyWhenNotCreate: true},
				),
				Column2: js.Items(
					js.Field{XType: "pbsD2DCalendarEvent", Label: "Schedule", Name: "schedule", EmptyText: "none (disabled)",
						DeleteEmptyWhenNotCreate: true, CBind: js.Obj{"value": "{scheduleValue}"}},
					js.Field{XType: "proxmoxtextfield", Label: "Number of retries", EmptyText: "0", Name: "retry"},
					js.Field{XType: "proxmoxtextfield", Label: "Retry interval (minutes)", EmptyText: "1", Name: "retry-interval"},
					js.Field{XType: "proxmoxtextfield", Label: "Max number of entries per directory", EmptyText: "1048576", Name: "max-dir-entries"},
					js.Field{XType: js.XCombo, Label: "Backup Mode", Name: "mode", QueryMode: "local", Store: js.Raw("backupModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true, AllowBlank: new(true),
						AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Metadata: store file metadata and directory structure only, skipping file contents. " +
							"Data: store file contents only, reusing metadata from previous backups. " +
							"Legacy: store everything in a single pass (slower, for compatibility).")},
						CBind: js.Obj{"value": "{backupModeValue}"}},
					js.Field{XType: js.XCombo, Label: "Source Mode", Name: "sourcemode", QueryMode: "local", Store: js.Raw("sourceModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true, AllowBlank: new(true),
						AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Snapshot: create a point-in-time snapshot of the source, then read from it. " +
							"Captures a consistent view even if files are in use during backup. " +
							"Direct: read files directly from the source without snapshotting. " +
							"Use Direct only when snapshotting is unavailable or not needed.")},
						CBind: js.Obj{"value": "{sourceModeValue}"}},
					js.Field{XType: js.XCombo, Label: "File Read Mode", Name: "readmode", QueryMode: "local", Store: js.Raw("readModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true, AllowBlank: new(true),
						CBind: js.Obj{"value": "{readModeValue}"}},
					js.Field{XType: js.XCombo, Label: "Extra security attributes", Name: "include-xattr", QueryMode: "local", Store: js.Raw("xattrModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true, AllowBlank: new(true),
						AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Controls whether extended security attributes (ACLs, owner, group, audit rules) " +
							"are included in the backup. Include to preserve full file permissions on restore. " +
							"Exclude if only basic file metadata is needed.")},
						CBind: js.Obj{"value": "{includeXAttrValue}"}},
					js.Field{XType: js.XCombo, Label: "Xattr Migration", Name: "legacy-xattr", QueryMode: "local", Store: js.Raw("legacyXattrModes"),
						DisplayField: "display", ValueField: "value", Editable: new(false), AnyMatch: true, ForceSelection: true, AllowBlank: new(true),
						AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Older PBS Plus versions stored extended attributes in a non-standard format. " +
							"'Use broken xattr' preserves compatibility with backups made by those versions. " +
							"'Use fixed xattr' uses the corrected format for new backups.")},
						CBind: js.Obj{"value": "{legacyXAttrValue}"}},
				),
				ColumnB: js.Items(
					js.Field{XType: "proxmoxtextfield", Label: "Comment", Name: "comment", DeleteEmptyWhenNotCreate: true},
					js.Field{XType: js.XTextArea, Name: "rawexclusions", Height: 150, Label: "Exclusions", Value: "",
						EmptyText: "Newline delimited list of exclusions following the .pxarexclude patterns."},
					js.Field{XType: "pbsD2DScriptSelector", Label: "Pre-Backup Script", Name: "pre_script"},
					js.Field{XType: "pbsD2DScriptSelector", Label: "Post-Backup Script", Name: "post_script"},
				),
			},
			js.Raw("PBS.D2DManagement.makeNotificationTab()"),
		),
	}),
}
