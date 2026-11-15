package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var snapshotBrowser = js.Define("PBS.window.D2DSnapshotBrowser", js.Obj{
	"extend": "PBS.plusWindow.Window",
	"alias":  "widget.pbsD2DSnapshotBrowser",

	"modal":        true,
	"width":        960,
	"height":       640,
	"layout":       "fit",
	"title":        js.T("Select Snapshot"),
	"defaultFocus": "[reference=hostFilter]",

	"config": js.Obj{
		"snapshotField": js.Raw("null"),
		"initialHost":   "",
	},

	"controller": js.Raw(`{
		xclass: "Ext.app.ViewController",

		init: function (view) {
			let me = this;
			let tree = me.lookup("tree");
			let store = view.getSnapshotField().getStore();
			view.mon(store, "beforeload", () => Proxmox.Utils.setErrorMask(tree, true));
			view.mon(store, "load", (s, records, success, operation) => {
				if (!success) {
					Proxmox.Utils.setErrorMask(tree, Proxmox.Utils.getResponseErrorMessage(operation.getError()));
					return;
				}
				Proxmox.Utils.setErrorMask(tree, false);
				me.refresh();
			});
			if (store.isLoading()) {
				Proxmox.Utils.setErrorMask(tree, true);
			}
			me.initFilters(view);
			me.refresh();
		},

		initFilters: function (view) {
			let me = this;
			let field = view.getSnapshotField();
			let targetOnly = me.lookup("targetOnly");
			let target = field.getRestoreTarget();
			let fields = [me.lookup("hostFilter"), targetOnly];
			fields.forEach((f) => f.suspendEvents(false));
			me.lookup("hostFilter").setValue(view.getInitialHost() || "");
			if (target) {
				let current = field.getSelection();
				targetOnly.setBoxLabel(Ext.String.format(gettext("Only backups of {0}"), Ext.htmlEncode(target)));
				targetOnly.setValue(!current || current.get("source_target") === target);
				targetOnly.show();
			}
			fields.forEach((f) => f.resumeEvents());
		},

		onFilterChange: function () {
			this.refresh();
		},

		setRange: function (btn) {
			let me = this;
			let from = me.lookup("fromDate");
			let to = me.lookup("toDate");
			from.suspendEvents(false);
			to.suspendEvents(false);
			if (btn.range === null) {
				from.setValue(null);
			} else {
				from.setValue(Ext.Date.add(Ext.Date.clearTime(new Date()), Ext.Date.DAY, -btn.range));
			}
			to.setValue(null);
			from.resumeEvents();
			to.resumeEvents();
			me.refresh();
		},

		clearFilters: function () {
			let me = this;
			let fields = ["hostFilter", "fromDate", "toDate", "latestOnly", "targetOnly"].map((ref) => me.lookup(ref));
			fields.forEach((f) => f.suspendEvents(false));
			me.lookup("hostFilter").setValue("");
			me.lookup("fromDate").setValue(null);
			me.lookup("toDate").setValue(null);
			me.lookup("latestOnly").setValue(false);
			me.lookup("targetOnly").setValue(false);
			fields.forEach((f) => f.resumeEvents());
			me.refresh();
		},

		reload: function () {
			this.getView().getSnapshotField().getStore().load();
		},

		expandAll: function () {
			this.lookup("tree").expandAll();
		},

		collapseAll: function () {
			this.lookup("tree").collapseAll();
		},

		refresh: function () {
			let me = this;
			let view = me.getView();
			let field = view.getSnapshotField();
			let tree = me.lookup("tree");
			let host = (me.lookup("hostFilter").getValue() || "").trim().toLowerCase();
			let from = me.lookup("fromDate").getValue();
			let to = me.lookup("toDate").getValue();
			let latest = me.lookup("latestOnly").getValue();
			let sourceTarget = me.lookup("targetOnly").getValue() ? field.getRestoreTarget() : null;
			let fromTs = from ? from.getTime() / 1000 : null;
			let toTs = to ? Ext.Date.add(Ext.Date.clearTime(to, true), Ext.Date.DAY, 1).getTime() / 1000 : null;
			let current = field.getValue();

			let exactHost = !!host && field.getStore().findBy((rec) => String(rec.get("backup-id")).toLowerCase() === host) !== -1;
			let groups = {};
			let available = 0;
			field.getStore().each((rec) => {
				let id = rec.get("backup-id");
				let time = rec.get("backup-time");
				if (!id || !time) {
					return;
				}
				available++;
				let hostId = String(id).toLowerCase();
				if (host && (exactHost ? hostId !== host : !hostId.includes(host))) {
					return;
				}
				if (sourceTarget && rec.get("source_target") !== sourceTarget) {
					return;
				}
				if (fromTs !== null && time < fromTs) {
					return;
				}
				if (toTs !== null && time >= toTs) {
					return;
				}
				(groups[id] = groups[id] || []).push(rec);
			});

			let names = Object.keys(groups).sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));
			let filtered = !!host || !!from || !!to || latest || !!sourceTarget;
			let expand = filtered || names.length <= 3;
			let total = 0;
			let children = names.map((name) => {
				let snaps = groups[name].sort((a, b) => b.get("backup-time") - a.get("backup-time"));
				let count = snaps.length;
				if (latest) {
					snaps = snaps.slice(0, 1);
				}
				total += snaps.length;
				let hasCurrent = !!current && snaps.some((r) => r.get("value") === current);
				return {
					id: "host:" + name,
					text: name,
					iconCls: "fa fa-server",
					leaf: false,
					expanded: expand || hasCurrent,
					count: count,
					time: snaps[0].get("backup-time"),
					children: snaps.map((r) => ({
						id: r.get("value"),
						value: r.get("value"),
						text: Ext.Date.format(new Date(r.get("backup-time") * 1000), "Y-m-d H:i:s"),
						iconCls: "fa fa-file-archive-o",
						leaf: true,
						time: r.get("backup-time"),
						size: r.get("size"),
						comment: r.get("comment"),
						verification: r.get("verification"),
						"protected": r.get("protected"),
					})),
				};
			});

			tree.getStore().setRoot({ expanded: true, children: children });
			me.lookup("summary").setText(Ext.String.format(gettext("{0} of {1} snapshots, {2} host(s)"), total, available, names.length));
			me.lookup("clearBtn").setDisabled(!filtered);
			me.lookup("selectBtn").setDisabled(true);

			if (current) {
				let node = tree.getStore().getNodeById(current);
				if (node) {
					tree.getSelectionModel().select(node);
					tree.ensureVisible(node);
				}
			}
		},

		selectionChanged: function () {
			let rec = this.lookup("tree").getSelection()[0];
			this.lookup("selectBtn").setDisabled(!rec || rec.get("leaf") !== true);
		},

		onItemDblClick: function (v, rec) {
			if (rec.get("leaf") === true) {
				this.onSelect();
			}
		},

		onItemKeyDown: function (v, rec, item, index, e) {
			if (e.getKey() === e.ENTER && rec.get("leaf") === true) {
				this.onSelect();
			}
		},

		onSelect: function () {
			let me = this;
			let view = me.getView();
			let rec = me.lookup("tree").getSelection()[0];
			if (!rec || rec.get("leaf") !== true) {
				return;
			}
			view.getSnapshotField().setValue(rec.get("value"));
			view.close();
		},
	}`),

	"dockedItems": js.Raw(`[
		{
			xtype: "toolbar",
			dock: "top",
			items: [
				{
					xtype: "textfield",
					reference: "hostFilter",
					emptyText: gettext("Filter by host"),
					width: 260,
					triggers: { clear: { cls: "pmx-clear-trigger", weight: -1, handler: function () { this.setValue(""); } } },
					listeners: { change: { fn: "onFilterChange", buffer: 200 } },
				},
				{ xtype: "checkbox", reference: "latestOnly", boxLabel: gettext("Latest per host"), margin: "0 0 0 10", listeners: { change: "onFilterChange" } },
				{ xtype: "checkbox", reference: "targetOnly", hidden: true, margin: "0 0 0 10", listeners: { change: "onFilterChange" } },
				"->",
				{ xtype: "button", iconCls: "fa fa-plus-square-o", tooltip: gettext("Expand All"), handler: "expandAll" },
				{ xtype: "button", iconCls: "fa fa-minus-square-o", tooltip: gettext("Collapse All"), handler: "collapseAll" },
				{ xtype: "button", iconCls: "fa fa-refresh", tooltip: gettext("Reload"), handler: "reload" },
			],
		},
		{
			xtype: "toolbar",
			dock: "top",
			items: [
				{ xtype: "datefield", reference: "fromDate", fieldLabel: gettext("From"), labelWidth: 40, width: 170, format: "Y-m-d", maxValue: new Date(), listeners: { change: "onFilterChange" } },
				{ xtype: "datefield", reference: "toDate", fieldLabel: gettext("To"), labelWidth: 25, width: 155, format: "Y-m-d", maxValue: new Date(), listeners: { change: "onFilterChange" } },
				{ xtype: "button", text: gettext("Today"), range: 0, handler: "setRange" },
				{ xtype: "button", text: gettext("7 days"), range: 6, handler: "setRange" },
				{ xtype: "button", text: gettext("30 days"), range: 29, handler: "setRange" },
				{ xtype: "button", text: gettext("All"), range: null, handler: "setRange" },
				{ xtype: "button", reference: "clearBtn", iconCls: "fa fa-filter", text: gettext("Clear Filters"), disabled: true, handler: "clearFilters" },
				"->",
				{ xtype: "tbtext", reference: "summary" },
			],
		},
	]`),

	"items": js.Raw(`[
		{
			xtype: "treepanel",
			reference: "tree",
			rootVisible: false,
			emptyText: gettext("No snapshots match the current filters."),
			store: { xclass: "Ext.data.TreeStore", proxy: { type: "memory" }, root: { expanded: true, children: [] } },
			listeners: { selectionchange: "selectionChanged", itemdblclick: "onItemDblClick", itemkeydown: "onItemKeyDown" },
			columns: [
				{
					xtype: "treecolumn",
					text: gettext("Host / Snapshot"),
					dataIndex: "text",
					flex: 2,
					renderer: function (v, meta, rec) {
						let text = Ext.String.htmlEncode(v);
						if (rec.get("leaf") !== true) {
							text += ' <span style="opacity: 0.6;">(' + rec.get("count") + ")</span>";
						}
						return text;
					},
				},
				{
					text: gettext("Latest"),
					dataIndex: "time",
					width: 150,
					renderer: function (v, meta, rec) {
						if (rec.get("leaf") === true || !v) {
							return "";
						}
						return Ext.Date.format(new Date(v * 1000), "Y-m-d H:i:s");
					},
				},
				{
					text: gettext("Size"),
					dataIndex: "size",
					width: 100,
					align: "end",
					renderer: function (v, meta, rec) {
						return rec.get("leaf") === true && v ? Proxmox.Utils.format_size(v) : "";
					},
				},
				{
					text: gettext("Verify State"),
					dataIndex: "verification",
					width: 110,
					renderer: function (v, meta, rec) {
						if (rec.get("leaf") !== true) {
							return "";
						}
						if (!v) {
							return '<i class="fa fa-question-circle-o"></i> ' + gettext("None");
						}
						if (v === "ok") {
							return '<i class="fa fa-check good"></i> ' + gettext("OK");
						}
						return '<i class="fa fa-times critical"></i> ' + gettext("Failed");
					},
				},
				{
					text: '<i class="fa fa-shield"></i>',
					tooltip: gettext("Protected"),
					dataIndex: "protected",
					width: 40,
					align: "center",
					renderer: function (v) {
						return v ? '<i class="fa fa-shield"></i>' : "";
					},
				},
				{
					text: gettext("Comment"),
					dataIndex: "comment",
					flex: 2,
					renderer: function (v) {
						return v ? Ext.String.htmlEncode(v) : "";
					},
				},
			],
		},
	]`),

	"buttons": js.Raw(`[
		{ text: gettext("Select"), reference: "selectBtn", disabled: true, handler: "onSelect" },
		{ text: gettext("Cancel"), handler: function (btn) { btn.up("window").close(); } },
	]`),
})
