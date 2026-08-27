package management

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var pathSelectors = []js.Value{
	js.Panel{
		Name: "PBS.form.D2DTargetPathSelector", XType: "pbsD2DTargetPathSelector", Extend: js.ExtFieldContainer, Layout: "hbox",
		Methods: map[string]js.Raw{
			"initComponent": js.Func("", `
				let me = this;
				me.items = [
					{ xtype: "proxmoxtextfield", name: me.name, reference: "destPathField", flex: 1, emptyText: gettext("/"), allowBlank: true, deleteEmpty: me.deleteEmpty },
					{ xtype: "button", iconCls: "fa fa-folder-open-o", margin: "0 0 0 5", handler: function () {
						if (!me.target) { Ext.Msg.alert(gettext("Error"), gettext("Please select a target first.")); return; }
						Ext.create("PBS.window.D2DPathSelector", {
							listURL: pbsPlusBaseUrl + "/api2/json/d2d/filetree/" + encodeURIComponent(encodePathValue(me.target)),
							prependSlash: false, onlyDirs: me.onlyDirs,
							listeners: { select: function (path) { me.down("proxmoxtextfield[reference=destPathField]").setValue(path); } },
						}).show();
					} },
				];
				me.callParent();
			`),
			"setTarget": js.Func("target", `this.target = target;`),
		},
	},
	js.Panel{
		Name: "PBS.form.D2DSnapshotPathSelector", XType: "pbsD2DSnapshotPathSelector", Extend: js.ExtFieldContainer, Layout: "hbox",
		Methods: map[string]js.Raw{
			"initComponent": js.Func("", `
				let me = this;
				me.items = [
					{ xtype: "proxmoxtextfield", name: "src-path", reference: "pathField", flex: 1, emptyText: gettext("/"), allowBlank: true, deleteEmpty: me.deleteEmpty },
					{ xtype: "button", iconCls: "fa fa-folder-open-o", margin: "0 0 0 5", handler: function (btn) {
						let me = btn.up("pbsD2DSnapshotPathSelector");
						let snapRecord = btn.up("pbsDiskRestoreJobEdit").lookup("snapshot").getSelection();
						if (!me.datastore || !me.snapshot || !snapRecord) { Ext.Msg.alert(gettext("Error"), gettext("Please select a valid snapshot first. A snapshot with an ongoing backup is considered invalid.")); return; }
						let files = snapRecord.get("files");
						let archive = files.find((f) => f.filename.endsWith(".mpxar.didx")) || files.find((f) => f.filename.endsWith(".pxar.didx"));
						if (!archive) { Ext.Msg.alert(gettext("Error"), gettext("No browsable archives found.")); return; }
						let parts = me.snapshot.split("/");
						Ext.create("PBS.window.D2DPathSelector", { listURL: "/api2/json/admin/datastore/" + encodeURIComponent(me.datastore) + "/catalog", extraParams: { "backup-type": parts[0], "backup-id": parts[1], "backup-time": parts[2], "archive-name": archive.filename, ns: me.ns || "" }, listeners: { select: function (path) { me.down("proxmoxtextfield[reference=pathField]").setValue(path); } } }).show();
					} },
				];
				me.callParent();
			`),
			"setDatastore": js.Func("datastore", `this.datastore = datastore;`),
			"setSnapshot":  js.Func("snapshot", `this.snapshot = snapshot;`),
			"setNamespace": js.Func("namespace", `this.ns = namespace;`),
		},
	},
}
