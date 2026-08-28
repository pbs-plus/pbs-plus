package mount

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

// archiveActionIcon shows icon only on mountable .pxar/.mpxar archives;
// archiveActionDisabled also refuses encrypted (crypt-mode 3) archives.
func archiveActionIcon(icon string) js.Raw {
	return js.Func("v, m, { data }", `
		if (data.ty === "file" && (data.filename.endsWith(".pxar.didx") || data.filename.endsWith(".mpxar.didx"))) {
			return "`+icon+`";
		}
		return "pmx-hidden";
	`)
}

var archiveActionDisabled = js.Func("v, r, c, i, { data }", `
	return !(data.ty === "file" &&
		(data.filename.endsWith(".pxar.didx") || data.filename.endsWith(".mpxar.didx")) &&
		data["crypt-mode"] < 3);
`)

var mountPanel = js.Panel{
	Name: "PBS.D2DSnapshotMount.DatastorePanel", XType: "pbsPlusSnapshotMountDatastorePanel",
	Extend:      "Ext.tree.Panel",
	Title:       "Content",
	ConfigProps: js.Obj{"datastore": nil},
	Mixins:      []string{"Proxmox.Mixin.CBind"},
	RootVisible: new(false),
	ViewConfig: &js.ViewConfig{GetRowClass: js.Func("record, index", `
		const verify = record.get("verification");
		if (verify && verify.lastFailed) {
			return "proxmox-invalid-row";
		}
		return null;
	`)},
	Listeners: js.Listeners{Activate: "onActivate", ItemContextMenu: "onItemContextMenu"},
	Controller: js.Controller{
		Control: js.Obj{
			"#":                    js.Obj{"rowdblclick": "rowDoubleClicked"},
			"pbsNamespaceSelector": js.Obj{"change": "nsChange"},
		},
		Methods: map[string]js.Raw{
			"init": js.Func("view", `
				if (!view.datastore) {
					throw "no datastore specified";
				}
				this.store = Ext.create("Ext.data.Store", {
					model: "pbs-data-store-snapshots",
					groupField: "backup-group",
				});
				this.store.on("load", this.onLoad, this);
				view.getStore().setSorters(["sortWeight", "text", "backup-time"]);
				this.reload();
			`),
			"onActivate": js.Func("", `
				const view = this.getView();
				// only load on first activate to not load every tab switch
				if (!view.firstLoad) {
					this.reload();
					view.firstLoad = true;
				}
			`),
			"onItemContextMenu": js.Func("panel, record, item, index, event", `
				event.stopEvent();
				let menu;
				let view = panel.up("pbsDataStoreContent");
				let controller = view.getController();
				let createControllerCallback = function (name) {
					return function () {
						controller[name](view, undefined, undefined, undefined, undefined, record);
					};
				};
				if (record.data.ty === "group") {
					menu = Ext.create("PBS.datastore.GroupCmdMenu", {
						title: gettext("Group"),
						onCopy: createControllerCallback("onCopy"),
					});
				} else if (record.data.ty === "dir") {
					menu = Ext.create("PBS.datastore.SnapshotCmdMenu", {
						title: gettext("Snapshot"),
						onCopy: createControllerCallback("onCopy"),
					});
				}
				if (menu) {
					menu.showAt(event.getXY());
				}
			`),
			"rowDoubleClicked": js.Func("table, rec, el, rowId, ev", `
				if (rec?.data?.ty === "ns" && !rec.data.root) {
					this.nsChange(null, rec.data.ns);
				}
			`),
			"nsChange": js.Func("field, value", `
				let view = this.getView();
				if (field === null) {
					field = view.down("pbsNamespaceSelector");
					field.setValue(value);
					return;
				}
				view.namespace = value;
				this.reload();
			`),
			"reload": js.Func("", `
				let view = this.getView();
				if (!view.store || !this.store) {
					console.warn("cannot reload, no store(s)");
					return;
				}
				let url = "/api2/json/admin/datastore/" + view.datastore + "/snapshots";
				if (view.namespace && view.namespace !== "") {
					url += "?ns=" + encodeURIComponent(view.namespace);
				}
				this.store.setProxy({
					type: "proxmox",
					// 5 minutes, we should make that api call faster
					timeout: 300 * 1000,
					url: url,
				});
				this.store.load();
			`),
			"unmountAll": js.Func("", `
				let me = this;
				let view = me.getView();
				let params = {};
				if (view.namespace && view.namespace !== "") {
					params.ns = view.namespace;
				}
				PBS.PlusUtils.API2Request({
					url: "/api2/extjs/config/d2d-unmount-all/" + encodeURIComponent(encodePathValue(view.datastore)),
					method: "POST",
					params,
					waitMsgTarget: view,
					failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
					success: () => Ext.toast(gettext("Unmount request sent")),
				});
			`),
			"s3Refresh": js.Func("", `
				let me = this;
				let view = me.getView();
				Proxmox.Utils.API2Request({
					url: "/admin/datastore/" + view.datastore + "/s3-refresh",
					method: "PUT",
					failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
					success: (response) => {
						Ext.create("Proxmox.window.TaskViewer", {
							upid: response.result.data,
							taskDone: () => me.reload(),
						}).show();
					},
				});
			`),
			"getRecordGroups": js.Func("records", `
				let groups = {};
				for (const item of records) {
					let btype = item.data["backup-type"];
					let group = btype + "/" + item.data["backup-id"];
					if (groups[group] !== undefined) {
						continue;
					}
					let cls = PBS.Utils.get_type_icon_cls(btype);
					if (cls === "") {
						console.warn("got unknown backup-type '" + btype + "'");
						// FIXME: auto render? what do?
						continue;
					}
					groups[group] = {
						text: group,
						leaf: false,
						iconCls: "fa " + cls,
						expanded: false,
						backup_type: item.data["backup-type"],
						backup_id: item.data["backup-id"],
						children: [],
					};
				}
				return groups;
			`),
			"updateGroupNotes": js.Func("async view", `
				try {
					if (!view || !view.store) {
						return;
					}
					let url = "/api2/extjs/admin/datastore/" + view.datastore + "/groups";
					if (view.namespace && view.namespace !== "") {
						url += "?ns=" + encodeURIComponent(view.namespace);
					}
					let { result: { data: groups } } = await Proxmox.Async.api2({ url });
					if (view.destroyed || !view.store) {
						return;
					}
					let map = {};
					for (const group of groups) {
						map[group["backup-type"] + "/" + group["backup-id"]] = group.comment;
					}
					let root = view.getRootNode();
					if (root) {
						root.cascade((node) => {
							if (node.data.ty === "group") {
								let group = node.data.backup_type + "/" + node.data.backup_id;
								node.set("comment", map[group], { dirty: false });
							}
						});
					}
				} catch (err) {
					console.debug(err);
				}
			`),
			"loadNamespaceFromSameLevel": js.Func("async", `
				let view = this.getView();
				try {
					let url = "/api2/extjs/admin/datastore/" + view.datastore + "/namespace?max-depth=1";
					if (view.namespace && view.namespace !== "") {
						url += "&parent=" + encodeURIComponent(view.namespace);
					}
					let { result: { data: ns } } = await Proxmox.Async.api2({ url });
					return ns;
				} catch (err) {
					console.debug(err);
				}
				return [];
			`),
			"onLoad": js.Func("async store, records, success, operation", `
				let me = this;
				let view = this.getView();
				if (!view || view.destroyed || !view.store) {
					return;
				}
				let namespaces = await me.loadNamespaceFromSameLevel();
				if (view.destroyed) {
					return;
				}
				if (!success) {
					// TODO also check error code for != 403 ?
					if (namespaces.length === 0) {
						let error = Proxmox.Utils.getResponseErrorMessage(operation.getError());
						Proxmox.Utils.setErrorMask(view.down("treeview"), error);
						return;
					} else {
						records = [];
					}
				} else {
					Proxmox.Utils.setErrorMask(view.down("treeview"));
				}
				let groups = this.getRecordGroups(records);
				let selected;
				let expanded = {};
				view.getSelection().some(function (item) {
					let id = item.data.text;
					if (item.data.leaf) {
						id = item.parentNode.data.text + id;
					}
					selected = id;
					return true;
				});
				view.getRootNode().cascadeBy({
					before: (item) => {
						if (item.isExpanded() && !item.data.leaf) {
							let id = item.data.text;
							expanded[id] = true;
							return true;
						}
						return false;
					},
					after: Ext.emptyFn,
				});
				for (const item of records) {
					let group = item.data["backup-type"] + "/" + item.data["backup-id"];
					let children = groups[group].children;
					let data = item.data;
					data.text = group + "/" + PBS.Utils.render_datetime_utc(data["backup-time"]);
					data.leaf = false;
					data.cls = "no-leaf-icons";
					data.matchesFilter = true;
					data.ty = "dir";
					data.expanded = !!expanded[data.text];
					data.children = [];
					for (const file of data.files) {
						file.text = file.filename;
						file["crypt-mode"] = PBS.Utils.cryptmap.indexOf(file["crypt-mode"]);
						file.fingerprint = data.fingerprint;
						file.leaf = true;
						file.matchesFilter = true;
						file.ty = "file";
						data.children.push(file);
					}
					children.push(data);
				}
				let nowSeconds = Date.now() / 1000;
				let children = [];
				for (const [name, group] of Object.entries(groups)) {
					let last_backup = 0;
					let crypt = { none: 0, mixed: 0, "sign-only": 0, encrypt: 0 };
					let verify = { outdated: 0, none: 0, failed: 0, ok: 0 };
					for (let item of group.children) {
						crypt[PBS.Utils.cryptmap[item["crypt-mode"]]]++;
						if (item["backup-time"] > last_backup && item.size !== null) {
							last_backup = item["backup-time"];
							group["backup-time"] = last_backup;
							group["last-comment"] = item.comment;
							group.files = item.files;
							group.size = item.size;
							group.owner = item.owner;
							verify.lastFailed = item.verification && item.verification.state !== "ok";
						}
						if (!item.verification) {
							verify.none++;
						} else {
							if (item.verification.state === "ok") {
								verify.ok++;
							} else {
								verify.failed++;
							}
							let task = Proxmox.Utils.parse_task_upid(item.verification.upid);
							item.verification.lastTime = task.starttime;
							if (nowSeconds - task.starttime > 30 * 24 * 60 * 60) {
								verify.outdated++;
							}
						}
					}
					group.verification = verify;
					group.count = group.children.length;
					group.matchesFilter = true;
					crypt.count = group.count;
					group["crypt-mode"] = PBS.Utils.calculateCryptMode(crypt);
					group.expanded = !!expanded[name];
					group.sortWeight = 0;
					group.ty = "group";
					children.push(group);
				}
				for (const item of namespaces) {
					if (item.ns === view.namespace || (!view.namespace && item.ns === "")) {
						continue;
					}
					children.push({
						text: item.ns,
						iconCls: "fa fa-object-group",
						expanded: true,
						expandable: false,
						ns: (view.namespaces ?? "") !== "" ? "/" + item.ns : item.ns,
						ty: "ns",
						sortWeight: 10,
						leaf: true,
					});
				}
				let isRootNS = !view.namespace || view.namespace === "";
				let rootText = isRootNS
					? gettext("Root Namespace")
					: Ext.String.format(gettext("Namespace '{0}'"), view.namespace);
				let topNodes = [];
				if (!isRootNS) {
					let parentNS = view.namespace.split("/").slice(0, -1).join("/");
					topNodes.push({
						text: ".. (" + (parentNS === "" ? gettext("Root") : parentNS) + ")",
						iconCls: "fa fa-level-up",
						ty: "ns",
						ns: parentNS,
						sortWeight: -10,
						leaf: true,
					});
				}
				topNodes.push({
					text: rootText,
					iconCls: "fa fa-" + (isRootNS ? "database" : "object-group"),
					expanded: true,
					expandable: false,
					sortWeight: -5,
					root: true,
					// fake root
					isRootNS,
					ty: "ns",
					children: children,
				});
				view.setRootNode({
					expanded: true,
					children: topNodes,
				});
				if (!children.length) {
					view.setEmptyText(
						Ext.String.format(
							gettext("No accessible snapshots found in namespace {0}"),
							view.namespace && view.namespace !== "" ? "'" + view.namespace + "'" : gettext("Root"),
						),
					);
				}
				this.updateGroupNotes(view);
				if (selected !== undefined) {
					let selection = view.getRootNode().findChildBy(
						function (item) {
							let id = item.data.text;
							if (item.data.leaf) {
								id = item.parentNode.data.text + id;
							}
							return selected === id;
						},
						undefined,
						true,
					);
					if (selection) {
						view.setSelection(selection);
						view.getView().focusRow(selection);
					}
				}
				Proxmox.Utils.setErrorMask(view, false);
				if (view.getStore().getFilters().length > 0) {
					let searchBox = me.lookup("searchbox");
					let searchvalue = searchBox.getValue();
					me.search(searchBox, searchvalue);
				}
			`),
			"onCopy": js.Func("async view, rI, cI, item, e, { data }", `
				await navigator.clipboard.writeText(data.text);
			`),
			"onNotesEdit": js.Func("view, data", `
				let me = this;
				let isGroup = data.ty === "group";
				let params;
				if (isGroup) {
					params = {
						"backup-type": data.backup_type,
						"backup-id": data.backup_id,
					};
				} else {
					params = {
						"backup-type": data["backup-type"],
						"backup-id": data["backup-id"],
						"backup-time": (data["backup-time"].getTime() / 1000).toFixed(0),
					};
				}
				if (view.namespace && view.namespace !== "") {
					params.ns = view.namespace;
				}
				Ext.create("PBS.window.NotesEdit", {
					url: "/admin/datastore/" + view.datastore + "/" + (isGroup ? "group-notes" : "notes"),
					autoShow: true,
					// FIXME: do something more efficient?
					apiCallDone: () => me.reload(),
					extraRequestParams: params,
				});
			`),
			"mountBackup": js.Func("tV, rI, cI, item, e, rec", `
				let me = this;
				let view = me.getView();
				if (!rec || rec.data.ty !== "file") {
					return;
				}
				let snapshot = rec.parentNode.data;
				let file = rec.data.filename;
				let isoTime = snapshot["backup-time"];
				if (isoTime instanceof Date) {
					// ensure ISO Z format
					isoTime = isoTime.toISOString().replace(/\.\d{3}Z$/, "Z");
				}
				let params = {
					"backup-id": snapshot["backup-id"],
					"backup-type": snapshot["backup-type"],
					"backup-time": isoTime,
					"file-name": file,
				};
				if (view.namespace && view.namespace !== "") {
					params.ns = view.namespace;
				}
				Ext.create("Ext.window.Window", {
					title: Ext.String.format(gettext("Mount '{0}'"), file),
					width: 460,
					modal: true,
					layout: "anchor",
					bodyPadding: 10,
					defaults: { anchor: "100%", labelWidth: 110 },
					items: [
						{
							xtype: "radiogroup",
							fieldLabel: gettext("Mode"),
							items: [
								{ boxLabel: gettext("Read-only"), name: "mode", inputValue: "ro", checked: true },
								{ boxLabel: gettext("Read-write (commit-capable)"), name: "mode", inputValue: "rw" },
							],
						},
						{
							xtype: "textfield",
							name: "mount-path",
							fieldLabel: gettext("Mount Path"),
							emptyText: gettext("Automatic (under /mnt/pbs-plus-restores)"),
						},
						{
							xtype: "displayfield",
							value: gettext("Custom paths must be empty directories under /mnt."),
						},
					],
					buttons: [
						{
							text: gettext("Mount"),
							 handler: function (btn) {
								let win = btn.up("window");
								let values = win.down("radiogroup").getValue();
								params.mode = values.mode;
								params["mount-path"] = win.down("textfield[name=mount-path]").getValue();
								PBS.PlusUtils.API2Request({
									url: "/api2/extjs/config/d2d-mount/" + encodeURIComponent(encodePathValue(view.datastore)),
									method: "POST",
									params,
									waitMsgTarget: view,
									failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
									success: (resp) => {
										win.close();
										Ext.create("PBS.plusWindow.TaskViewer", {
											upid: resp.result.data,
										}).show();
									},
									});
								},
							},
					],
				}).show();
			`),
			"unmountBackup": js.Func("tV, rI, cI, item, e, rec", `
				let me = this;
				let view = me.getView();
				let snapshot = rec && rec.parentNode ? rec.parentNode.data : null;
				let fileRec = rec && rec.data && rec.data.ty === "file" ? rec.data : null;
				if (!snapshot || !fileRec) {
					Ext.Msg.alert(gettext("Error"), gettext("Please select a file entry to unmount."));
					return;
				}
				let isoTime = snapshot["backup-time"];
				if (isoTime instanceof Date) {
					// ensure ISO Z format
					isoTime = isoTime.toISOString().replace(/\.\d{3}Z$/, "Z");
				}
				let params = {
					"backup-id": snapshot["backup-id"],
					"backup-type": snapshot["backup-type"],
					"backup-time": isoTime,
					"file-name": fileRec.filename,
					force: 1,
				};
				if (view.namespace && view.namespace !== "") {
					params.ns = view.namespace;
				}
				Ext.Msg.confirm(
					gettext("Confirm"),
					Ext.String.format(gettext("Unmount '{0}'? Uncommitted changes of read-write mounts are lost."), fileRec.filename),
					(btn) => {
						if (btn !== "yes") return;
						PBS.PlusUtils.API2Request({
							url: "/api2/extjs/config/d2d-unmount/" + encodeURIComponent(encodePathValue(view.datastore)),
							method: "POST",
							params,
							waitMsgTarget: view,
							failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
							success: (resp) => {
								Ext.create("PBS.plusWindow.TaskViewer", {
									upid: resp.result.data,
								}).show();
							},
						});
					},
				);
			`),
			"openBrowser": js.Func("tv, rI, Ci, item, e, rec", `
				let me = this;
				let view = me.getView();
				if (rec.data.ty === "ns") {
					me.nsChange(null, rec.data.ns);
					return;
				}
				if (rec?.data?.ty !== "file") {
					return;
				}
				let snapshot = rec.parentNode.data;
				let id = snapshot["backup-id"];
				let time = snapshot["backup-time"];
				let type = snapshot["backup-type"];
				let timetext = PBS.Utils.render_datetime_utc(snapshot["backup-time"]);
				let extraParams = {
					"backup-id": id,
					"backup-time": (time.getTime() / 1000).toFixed(0),
					"backup-type": type,
				};
				if (rec.data.filename.endsWith(".mpxar.didx")) {
					extraParams["archive-name"] = rec.data.filename;
				}
				if (view.namespace && view.namespace !== "") {
					extraParams.ns = view.namespace;
				}
				Ext.create("Proxmox.window.FileBrowser", {
					title: type + "/" + id + "/" + timetext,
					listURL: "/api2/json/admin/datastore/" + view.datastore + "/catalog",
					downloadURL: "/api2/json/admin/datastore/" + view.datastore + "/pxar-file-download",
					extraParams,
					enableTar: true,
					downloadPrefix: type + "-" + id + "-",
					archive: rec.data.filename,
				}).show();
			`),
			"composeSnapshot": js.Func("tv, rI, cI, item, e, rec", `
				let me = this;
				let view = me.getView();
				if (!rec || rec.data.ty !== "file") {
					return;
				}
				let snapshot = rec.parentNode.data;
				let isoTime = snapshot["backup-time"];
				if (isoTime instanceof Date) {
					isoTime = isoTime.toISOString().replace(/\.\d{3}Z$/, "Z");
				}
				Ext.create("PBS.D2DSnapshotMount.ComposeWindow", {
					datastore: view.datastore,
					namespace: view.namespace || "",
					backupId: snapshot["backup-id"],
					backupType: snapshot["backup-type"],
					backupTime: isoTime,
					archive: rec.data.filename,
					autoShow: true,
				});
			`),
			"filter": js.Func("item, value", `
				if (item.data.text.indexOf(value) !== -1) {
					return true;
				}
				if (item.data.owner && item.data.owner.indexOf(value) !== -1) {
					return true;
				}
				return false;
			`),
			"search": js.Func("tf, value", `
				let me = this;
				let view = me.getView();
				let store = view.getStore();
				if (!value && value !== 0) {
					store.clearFilter();
					// only collapse the children below our toplevel namespace "root"
					store.getRoot().lastChild.collapseChildren(true);
					tf.triggers.clear.setVisible(false);
					return;
				}
				tf.triggers.clear.setVisible(true);
				if (value.length < 2) {
					return;
				}
				Proxmox.Utils.setErrorMask(view, true);
				// we do it a little bit later for the error mask to work
				setTimeout(function () {
					store.clearFilter();
					store.getRoot().collapseChildren(true);
					store.beginUpdate();
					store.getRoot().cascadeBy({
						before: function (item) {
							if (me.filter(item, value)) {
								item.set("matchesFilter", true);
								if (item.parentNode && item.parentNode.id !== "root") {
									item.parentNode.childmatches = true;
								}
								return false;
							}
							return true;
						},
						after: function (item) {
							if (me.filter(item, value) || item.id === "root" || item.childmatches) {
								item.set("matchesFilter", true);
								if (item.parentNode && item.parentNode.id !== "root") {
									item.parentNode.childmatches = true;
								}
								if (item.childmatches) {
									item.expand();
								}
							} else {
								item.set("matchesFilter", false);
							}
							delete item.childmatches;
						},
					});
					store.endUpdate();
					store.filter((item) => !!item.get("matchesFilter"));
					Proxmox.Utils.setErrorMask(view, false);
				}, 10);
			`),
		},
	},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reload"},
		{XType: js.XButton, Text: "Unmount All", IconCls: "fa fa-eject", Handler: "unmountAll"},
		{XType: js.XButton, Text: "More", ItemID: "moreDropdown", Hidden: true, Menu: js.Arr{js.Obj{
			"text": js.T("Refresh contents from S3 bucket"), "iconCls": "fa fa-cloud-download",
			"handler": "s3Refresh", "selModel": false,
		}}},
		js.Fill(),
		{XType: js.XTbText, HTMLRaw: js.Raw(`gettext("Namespace") + ":"`)},
		{XType: "pbsNamespaceSelector", Width: 200, CBind: js.Obj{"datastore": "{datastore}"}},
		js.Sep(),
		{XType: js.XTbText, HTMLRaw: js.T("Search")},
		{XType: js.XTextField, Reference: "searchbox", EmptyText: "group, date or owner",
			ClearTrigger: &js.ClearTrigger{Cls: "pmx-clear-trigger", Weight: -1, Hidden: true}, Change: "search"},
	},
	Columns: []js.Column{
		{XType: js.XTreeColumn, Text: "Backup Group", DataIndex: "text", Flex: 1, Renderer: js.Func("value, meta, record", `
			if (record.data.protected) {
				return value + " (" + gettext("protected") + ")";
			}
			return value;
		`)},
		{Text: "Comment", DataIndex: "comment", Flex: 1, Renderer: js.Func("v, meta, record", `
			let data = record.data;
			if (!data || data.leaf || data.root) {
				return "";
			}
			let additionalClasses = "";
			if (!v) {
				if (!data.expanded) {
					v = data["last-comment"] ?? "";
					additionalClasses = "pmx-opacity-75";
				} else {
					v = "";
				}
			}
			v = Ext.String.htmlEncode(v);
			let icon = "x-action-col-icon fa fa-fw fa-pencil pointer";
			return '<span class="snapshot-comment-column ' + additionalClasses + '">' + v + '</span>' +
				'    <i data-qtip="' + gettext("Edit") + '" style="float: right; margin: 0px;" class="' + icon + '"></i>';
		`), Listeners: js.Obj{
			"afterrender": js.Raw(`function (component) {
				// one handler for the whole column is cheaper than one per icon
				component.on("click", function (tree, cell, rowI, colI, e, rec) {
					let el = e.target;
					if (el.tagName !== "I" || !el.classList.contains("fa-pencil")) {
						return;
					}
					let view = tree.up();
					let controller = view.controller;
					controller.onNotesEdit(view, rec.data);
				});
			}`),
			"dblclick": js.Raw(`function (tree, el, row, col, ev, rec) {
				let data = rec.data || {};
				if (data.leaf || data.root) {
					return;
				}
				let view = tree.up();
				let controller = view.controller;
				controller.onNotesEdit(view, rec.data);
			}`),
		}},
		{XType: js.XActionColumn, Text: "Actions", DataIndex: "text", Width: 150, Items: js.Arr{
			js.Obj{
				"handler":          "mountBackup",
				"getTip":           js.Func("v, m, rec", `return Ext.String.format(gettext("Mount '{0}'"), v);`),
				"getClass":         archiveActionIcon("fa fa-hdd-o"),
				"isActionDisabled": archiveActionDisabled,
			},
			js.Obj{
				"handler":          "unmountBackup",
				"getTip":           js.Func("v, m, rec", `return Ext.String.format(gettext("Unmount '{0}'"), v);`),
				"getClass":         archiveActionIcon("fa fa-eject"),
				"isActionDisabled": archiveActionDisabled,
			},
			js.Obj{
				"handler": "openBrowser",
				"tooltip": js.T("Browse"),
				"getClass": js.Func("v, m, { data }", `
					if ((data.ty === "file" && (data.filename.endsWith(".pxar.didx") || data.filename.endsWith(".mpxar.didx"))) ||
						(data.ty === "ns" && !data.root)) {
						return "fa fa-folder-open-o";
					}
					return "pmx-hidden";
				`),
				"isActionDisabled": js.Func("v, r, c, i, { data }", `
					return !(data.ty === "file" &&
						(data.filename.endsWith(".pxar.didx") || data.filename.endsWith(".mpxar.didx")) &&
						data["crypt-mode"] < 3) && data.ty !== "ns";
				`),
			},
			js.Obj{
				"handler":          "composeSnapshot",
				"getTip":           js.Func("v, m, rec", `return Ext.String.format(gettext("Compose new snapshot from '{0}'"), v);`),
				"getClass":         archiveActionIcon("fa fa-files-o"),
				"isActionDisabled": archiveActionDisabled,
			},
		}},
		{XType: js.XDateColumn, Text: "Backup Time", DataIndex: "backup-time", Format: "Y-m-d H:i:s", Width: 150, Sortable: new(true)},
		{Text: "Size", DataIndex: "size", Sortable: new(true), Renderer: js.Func("v, meta, { data }", `
			if ((data.text === "client.log.blob" && v === undefined) || (data.ty !== "dir" && data.ty !== "file")) {
				return "";
			}
			if (v === undefined || v === null) {
				meta.tdCls = "x-grid-row-loading";
				return "";
			}
			return Proxmox.Utils.format_size(v);
		`)},
		{XType: js.XNumberColumn, Format: "0", Text: "Count", DataIndex: "count", Width: 75, Align: "right", Sortable: new(true)},
		{Text: "Encrypted", DataIndex: "crypt-mode", Renderer: js.Func("v, meta, record", `
			if (record.data.size === undefined || record.data.size === null) {
				return "";
			}
			if (v === -1) {
				return "";
			}
			let iconCls = PBS.Utils.cryptIconCls[v] || "";
			let iconTxt = "";
			if (iconCls) {
				iconTxt = '<i class="fa fa-fw fa-' + iconCls + '"></i> ';
			}
			let tip;
			if (v !== PBS.Utils.cryptmap.indexOf("none") && record.data.fingerprint !== undefined) {
				tip = "Key: " + PBS.Utils.renderKeyID(record.data.fingerprint);
			}
			let txt = iconTxt + PBS.Utils.cryptText[v] || Proxmox.Utils.unknownText;
			if (record.data.ty === "group" || tip === undefined) {
				return txt;
			} else {
				return '<span data-qtip="' + tip + '">' + txt + '</span>';
			}
		`)},
	},
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			let me = this;
			me.callParent();
			Proxmox.Utils.API2Request({
				url: "/config/datastore/" + me.datastore,
				failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
				success: (response) => {
					let data = response.result.data;
					if (data.backend) {
						let backendConfig = PBS.Utils.parsePropertyString(data.backend);
						let hasS3Backend = backendConfig.type === "s3";
						me.down("#moreDropdown").setHidden(!hasS3Backend);
					}
				},
			});
		`),
	},
}
