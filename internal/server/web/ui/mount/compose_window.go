package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var composeWindow = js.Define("PBS.D2DSnapshotMount.ComposeWindow", js.Obj{
	"extend": "Ext.window.Window",
	"alias":  "widget.pbsPlusComposeWindow",

	"modal":  true,
	"width":  700,
	"height": 600,
	"layout": "fit",

	"config": js.Obj{
		"datastore":  nil,
		"namespace":  "",
		"backupId":   "",
		"backupType": "",
		"backupTime": "",
		"archive":    "",
	},

	"controller": js.Raw(`{
		xclass: 'Ext.app.ViewController',

		compose: function(btn) {
			let me = this;
			let view = me.getView();
			let tree = me.lookup("tree");
			let sel = tree.getSelection();
			if (!sel || sel.length < 1) {
				Ext.Msg.alert(gettext("Error"), gettext("Select at least one file or directory."));
				return;
			}
			let form = me.lookup("form");
			if (!form.isValid()) return;
			let vals = form.getValues();
			if (vals["strip-root"] === "1") {
				if (sel.length !== 1) {
					Ext.Msg.alert(gettext("Error"), gettext("Flatten mode requires exactly one selected directory."));
					return;
				}
				if (sel[0].data.leaf) {
					Ext.Msg.alert(gettext("Error"), gettext("Flatten mode requires a directory, not a file."));
					return;
				}
			}
			let params = {
				ns: view.namespace || "",
				"backup-type": view.backupType,
				"backup-id": view.backupId,
				"backup-time": view.backupTime,
				"file-name": view.archive,
				"target-ns": vals["target-ns"] || "",
				"target-type": vals["target-type"] || "host",
				"target-id": vals["target-id"],
				"paths": sel
					.map((rec) => rec.data.filepath)
					.filter((fp) => !!fp)
					.map((fp) => {
						let p = atob(fp);
						if (!p.startsWith("/")) p = "/" + p;
						return btoa(p);
					})
					.join(","),
			};
			if (vals["strip-root"] === "1") params["strip-root"] = "1";
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/d2d-compose/" + encodeURIComponent(encodePathValue(view.datastore)),
				method: "POST",
				params: params,
				waitMsgTarget: view,
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
				success: (resp) => {
					view.close();
					Ext.create("PBS.plusWindow.TaskViewer", { upid: resp.result.data }).show();
				},
			});
		},

		init: function(view) {
			let me = this;
			let idField = me.lookup("form").getForm().findField("target-id");
			if (idField && !idField.getValue()) idField.setValue(view.backupId);
			let tree = me.lookup("tree");
			let store = tree.getStore();
			let proxy = store.getProxy();
			let extraParams = {
				"backup-id": view.backupId,
				"backup-type": view.backupType,
				"backup-time": (Date.parse(view.backupTime) / 1000).toFixed(0),
			};
			if (view.archive) {
				extraParams["archive-name"] = view.archive;
			}
			if (view.namespace && view.namespace !== "") {
				extraParams.ns = view.namespace;
			}
			proxy.setUrl("/api2/json/admin/datastore/" + view.datastore + "/catalog");
			proxy.setTimeout(60 * 1000);
			proxy.setExtraParams(extraParams);
			store.load(() => {
				let root = store.getRoot();
				root.expand();
				if (view.archive) {
					let child = root.findChild("text", view.archive);
					if (child) child.expand();
				} else if (root.childNodes.length === 1) {
					root.firstChild.expand();
				}
			});
		},
	}`),

	"items": js.Arr{
		js.Obj{
			"xtype":       "form",
			"reference":   "form",
			"border":      false,
			"layout":      js.Obj{"type": "vbox", "align": "stretch"},
			"bodyPadding": 10,
			"defaults":    js.Obj{"anchor": "100%", "labelWidth": 120},
			"items": js.Arr{
				js.Obj{
					"xtype": "displayfield",
					"value": "Select files or directories (Ctrl-click for multiple) to compose into a new snapshot. Existing chunks are reused; nothing is modified in the source.",
				},
				js.Obj{
					"xtype": "proxmoxcheckbox", "name": "strip-root", "fieldLabel": "Flatten",
					"boxLabel":       "Use contents of a single selected directory as snapshot root",
					"uncheckedValue": "0", "inputValue": "1", "margin": "0 0 5 0",
				},
				js.Obj{
					"xtype":  "fieldcontainer",
					"layout": js.Obj{"type": "hbox", "align": "center"},
					"margin": "5 0",
					"items": js.Arr{
						js.Obj{
							"xtype": "proxmoxtextfield", "name": "target-ns", "fieldLabel": "Target Namespace",
							"emptyText": "root", "flex": 1, "labelWidth": 120,
						},
						js.Obj{"xtype": "tbspacer", "width": 10},
						js.Obj{
							"xtype": "combobox", "name": "target-type", "fieldLabel": "Target Type",
							"store": js.Raw(`[["host", "host"], ["vm", "vm"], ["ct", "ct"]]`),
							"value": "host", "labelWidth": 90, "width": 180, "editable": false,
						},
						js.Obj{"xtype": "tbspacer", "width": 10},
						js.Obj{
							"xtype": "proxmoxtextfield", "name": "target-id", "fieldLabel": "Target ID",
							"allowBlank": false, "flex": 1, "labelWidth": 80,
						},
					},
				},
				js.Obj{
					"xtype":       "treepanel",
					"reference":   "tree",
					"flex":        1,
					"rootVisible": false,
					"selModel":    js.Raw(`{ type: "treemodel", mode: "MULTI" }`),
					"store": js.Obj{
						"autoLoad":      false,
						"model":         "proxmox-file-tree",
						"defaultRootId": "/",
						"nodeParam":     "filepath",
						"sorters":       "text",
						"proxy": js.Obj{
							"appendId": false,
							"type":     "proxmox",
						},
					},
					"columns": js.Raw(`[
						{ text: gettext("Name"), xtype: "treecolumn", flex: 1, dataIndex: "text", renderer: Ext.String.htmlEncode },
						{ text: gettext("Size"), dataIndex: "sizedisplay", align: "end" },
					]`),
				},
			},
		},
	},

	"buttons": js.Arr{
		js.Obj{"text": "Cancel", "handler": js.Raw(`function(btn) { btn.up("window").close(); }`)},
		js.Obj{"text": "Create Snapshot", "handler": "compose", "reference": "composeBtn"},
	},
})
