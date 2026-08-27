package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var pathBrowserWindow = js.Panel{
	Name: "PBS.window.D2DPathSelector", XType: "pbsD2DPathSelector",
	Extend:      "Proxmox.window.FileBrowser",
	Title:       "Select Path",
	ConfigProps: js.Obj{"prependSlash": true, "onlyDirs": false},
	Controller: js.Controller{
		Control: js.Obj{
			"treepanel": js.Obj{
				"selectionchange": "fileChanged",
				"itemdblclick": js.Func("v, rec", `
					if (!this.getView().getOnlyDirs() || rec.get("leaf") !== true) {
						this.onSelect();
					}
				`),
			},
		},
		Methods: map[string]js.Raw{
			"init": js.Func("view", `
				let me = this;
				let tree = me.lookup("tree");
				if (!view.listURL) {
					throw "no list URL given";
				}
				let store = tree.getStore();
				let proxy = store.getProxy();
				let errorCallback = (error, msg) => me.errorHandler(error, msg);
				proxy.setUrl(view.listURL);
				proxy.setTimeout(60 * 1000);
				proxy.setExtraParams(view.extraParams);
				proxy.setWithCredentials(true);
				proxy.setHeaders(
					Ext.apply(proxy.getHeaders() || {}, {
						Accept: "application/json",
					}),
				);
				tree.mon(store, "beforeload", () => {
					Proxmox.Utils.setErrorMask(tree, true);
				});
				tree.mon(store, "load", (treestore, rec, success, operation, node) => {
					if (success) {
						Proxmox.Utils.setErrorMask(tree, false);
						return;
					}
					// A 503 while listing can be transient; retry the node a few times.
					if (operation?.error?.status === 503 && node.loadCount < 10) {
						node.collapse();
						node.expand();
						node.loadCount = (node.loadCount || 0) + 1;
						return;
					}
					let error = operation.getError();
					let msg = Proxmox.Utils.getResponseErrorMessage(error);
					if (!errorCallback(error, msg)) {
						Proxmox.Utils.setErrorMask(tree, msg);
					} else {
						Proxmox.Utils.setErrorMask(tree, false);
					}
				});
				store.load((rec, op, success) => {
					let root = store.getRoot();
					root.expand();
					if (root.childNodes.length === 1) {
						root.firstChild.expand();
					}
					me.initialLoadDone = success;
				});
			`),
			"errorHandler": js.Func("error, msg", `
				if (error?.status === 503) return false;
				if (this.initialLoadDone) {
					Ext.Msg.alert(gettext("Error"), msg);
					return true;
				}
				return false;
			`),
			"fileChanged": js.Func("", `
				let view = this.getView();
				let tree = this.lookup("tree");
				let selection = tree.getSelection();
				let canSelect = selection && selection.length > 0;
				if (canSelect && view.getOnlyDirs()) {
					let rec = selection[0];
					if (rec.get("leaf") === true) {
						canSelect = false;
					}
				}
				this.lookup("selectBtn").setDisabled(!canSelect);
			`),
			"onSelect": js.Func("", `
				let view = this.getView();
				let tree = this.lookup("tree");
				let selection = tree.getSelection();
				if (selection && selection.length > 0) {
					let rec = selection[0];
					if (view.getOnlyDirs() && rec.get("leaf") === true) {
						return;
					}
					let data = rec.data;
					try {
						let path = atob(data.filepath);
						if (view.getPrependSlash() && !path.startsWith("/")) {
							path = "/" + path;
						}
						view.fireEvent("select", path);
						view.close();
					} catch (e) {
						console.error("Failed to decode path:", data.filepath);
					}
				}
			`),
		},
	},
	Fbar: []js.Tool{
		{XType: js.XButton, Text: "Select", Reference: "selectBtn", Disabled: true, Handler: "onSelect"},
		{XType: js.XButton, Text: "Cancel", HandlerFn: js.Func("", `this.up("window").close();`)},
	},
}
