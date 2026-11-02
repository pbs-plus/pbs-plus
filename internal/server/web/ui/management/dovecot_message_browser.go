package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var dovecotMessageBrowser = js.Define("PBS.window.D2DDovecotMessageSelector", js.Obj{
	"extend": "Ext.window.Window",
	"alias":  "widget.pbsD2DDovecotMessageSelector",

	"modal":  true,
	"width":  700,
	"height": 600,
	"layout": "fit",
	"title":  "Select Messages",

	"config": js.Obj{
		"listURL":     "",
		"extraParams": js.Obj{},
	},

	"controller": js.Raw(`{
		xclass: 'Ext.app.ViewController',

		init: function(view) {
			let me = this;
			let tree = me.lookup("tree");
			if (!view.listURL) {
				throw "no list URL given";
			}
			let store = tree.getStore();
			let proxy = store.getProxy();
			proxy.setUrl(view.listURL);
			proxy.setTimeout(60 * 1000);
			proxy.setExtraParams(view.extraParams);
			proxy.setWithCredentials(true);
			proxy.setHeaders(Ext.apply(proxy.getHeaders() || {}, { Accept: "application/json" }));
			tree.mon(store, "beforeload", () => Proxmox.Utils.setErrorMask(tree, true));
			tree.mon(store, "load", (treestore, rec, success, operation) => {
				if (success) {
					Proxmox.Utils.setErrorMask(tree, false);
					return;
				}
				let msg = Proxmox.Utils.getResponseErrorMessage(operation.getError());
				Proxmox.Utils.setErrorMask(tree, msg);
			});
			store.load(() => {
				let root = store.getRoot();
				root.expand();
				if (root.childNodes.length === 1) {
					root.firstChild.expand();
				}
			});
		},

		selectionChanged: function() {
			let sel = this.lookup("tree").getView().getSelection() || [];
			let messages = sel.filter((rec) => rec.get("leaf") === true);
			this.lookup("selectBtn").setDisabled(messages.length === 0);
		},

		onSelect: function() {
			let me = this;
			let view = me.getView();
			let sel = me.lookup("tree").getView().getSelection() || [];
			let paths = [];
			let skipped = 0;
			sel.forEach((rec) => {
				if (rec.get("leaf") !== true) return;
				let encoded = rec.data.filepath;
				if (!encoded) return;
				let path;
				try {
					path = atob(encoded);
				} catch (e) {
					console.error("Failed to decode path:", encoded);
					return;
				}

				let rooted = path.replace(/^[/]+/, "");
				let relative = rooted.replace(/^(?:users[/][^/]+[/])?mail[/]/, "");
				if (relative === rooted || relative === "") {
					skipped += 1;
					return;
				}
				paths.push(relative);
			});
			if (paths.length === 0) {
				Ext.Msg.alert(gettext("Error"), gettext("Select at least one message from a user's mail directory."));
				return;
			}
			if (skipped > 0) {
				Ext.Msg.alert(gettext("Warning"), Ext.String.format(gettext("Skipped {0} file(s) outside a mail directory."), skipped));
			}
			view.fireEvent("select", paths);
			view.close();
		},
	}`),

	"items": js.Arr{
		js.Obj{
			"xtype":       "treepanel",
			"reference":   "tree",
			"rootVisible": false,
			"selModel":    js.Raw(`{ type: "treemodel", mode: "MULTI" }`),
			"listeners":   js.Raw(`{ selectionchange: "selectionChanged", itemdblclick: function(v, rec) { if (rec.get("leaf") === true) { this.up("window").getController().onSelect(); } } }`),
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

	"buttons": js.Arr{
		js.Obj{"text": "Add Selected", "reference": "selectBtn", "disabled": true, "handler": "onSelect"},
		js.Obj{"text": "Cancel", "handler": js.Raw(`function(btn) { btn.up("window").close(); }`)},
	},
})

var dovecotMessageSelector = js.Panel{
	Name: "PBS.form.D2DDovecotMessageSelector", XType: "pbsD2DDovecotMessageSelector",
	Extend: js.ExtFieldContainer, Layout: "hbox",
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			let me = this;
			me.items = [
				{
					xtype: "textarea", name: "dovecot_messages", reference: "messagesField", flex: 1, height: 110,
					allowBlank: true, deleteEmpty: me.deleteEmpty,
					emptyText: gettext("Newline delimited Maildir paths, for example .Sent/cur/1700000000.M1P2.mail:2,S"),
				},
				{ xtype: "button", iconCls: "fa fa-folder-open-o", margin: "0 0 0 5", handler: function (btn) {
					let me = btn.up("pbsD2DDovecotMessageSelector");
					let edit = btn.up("pbsDiskRestoreJobEdit");
					let snapRecord = edit ? edit.lookup("snapshot").getSelection() : null;
					if (!me.datastore || !me.snapshot || !snapRecord) {
						Ext.Msg.alert(gettext("Error"), gettext("Please select a valid snapshot first. A snapshot with an ongoing backup is considered invalid."));
						return;
					}
					let files = snapRecord.get("files");
					let archive = files.find((f) => f.filename.endsWith(".mpxar.didx")) || files.find((f) => f.filename.endsWith(".pxar.didx"));
					if (!archive) { Ext.Msg.alert(gettext("Error"), gettext("No browsable archives found.")); return; }
					let parts = me.snapshot.split("/");
					Ext.create("PBS.window.D2DDovecotMessageSelector", {
						listURL: "/api2/json/admin/datastore/" + encodeURIComponent(me.datastore) + "/catalog",
						extraParams: { "backup-type": parts[0], "backup-id": parts[1], "backup-time": parts[2], "archive-name": archive.filename, ns: me.ns || "" },
						listeners: { select: function (paths) {
							let field = me.down("textarea[reference=messagesField]");
							let existing = (field.getValue() || "").split("\n").map((l) => l.trim()).filter((l) => l !== "");
							paths.forEach((p) => { if (!existing.includes(p)) existing.push(p); });
							field.setValue(existing.join("\n"));
						} },
					}).show();
				} },
			];
			me.callParent();
		`),
		"setDatastore": js.Func("datastore", `this.datastore = datastore;`),
		"setSnapshot":  js.Func("snapshot", `this.snapshot = snapshot;`),
		"setNamespace": js.Func("namespace", `this.ns = namespace;`),
	},
}
