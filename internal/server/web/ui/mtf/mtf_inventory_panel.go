package mtf

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

const mtfScanURL = "/api2/extjs/config/mtf-scan"

var mtfScanWindow = js.Func("", `
	let view = this.getView();
	Ext.create("PBS.plusWindow.Edit", {
		title: gettext("MTF Inventory Scan"),
		method: "POST",
		url: "`+mtfScanURL+`",
		isCreate: true,
		submitText: gettext("Start Scan"),
		autoShow: true,
		width: 450,
		submit: function () {
			let me = this;
			let values = me.getValues();
			delete values._source_type;
			delete values.delete;
			if (Ext.isArray(values.barcodes)) {
				values.barcodes = values.barcodes.join(",");
			}
			PBS.PlusUtils.API2Request({
				url: me.url,
				method: me.method || "POST",
				params: values,
				waitMsgTarget: me,
				success: function (response) {
					me.hide();
					Ext.create("PBS.plusWindow.TaskViewer", {
						upid: response.result.data,
						taskDone: function () {
							view.getStore().rstore.load();
							me.close();
						},
					}).show();
				},
				failure: (response) => Ext.Msg.alert(gettext("Error"), response.htmlStatus),
			});
		},
		items: [
			{
				xtype: "combobox",
				name: "_source_type",
				submitValue: false,
				fieldLabel: gettext("Source"),
				labelWidth: 100,
				editable: false,
				value: "changer",
				store: [
					["changer", gettext("Changer")],
					["drive", gettext("Standalone Drive")],
					["bkf", gettext(".bkf File or Directory")],
				],
				listeners: {
					change: function (cb, val) {
						let win = cb.up("window");
						win.down("#scanChangerCt").setVisible(val === "changer");
						win.down("#scanDriveCt").setVisible(val === "changer" || val === "drive");
						win.down("#scanBkfCt").setVisible(val === "bkf");
					},
				},
			},
			{
				xtype: "container",
				itemId: "scanChangerCt",
				layout: "anchor",
				defaults: { anchor: "100%", labelWidth: 100 },
				items: [
					{
						xtype: "combobox",
						name: "changer",
						fieldLabel: gettext("Changer"),
						allowBlank: false,
						editable: false,
						displayField: "name",
						valueField: "path",
						store: {
							fields: ["name", "path"],
							autoLoad: true,
							proxy: { type: "proxmox", url: "/api2/json/tape/changer", queryParam: null },
						},
						listeners: {
							change: function (cb) {
								let win = cb.up("window");
								let driveCombo = win.down("#scanDriveField");
								if (driveCombo) driveCombo.setValue("");
								let bcField = win.down("#scanBarcodeField");
								if (!bcField) return;
								let path = cb.getValue();
								bcField.setValue("");
								bcField.getStore().loadData([]);
								if (!path) return;
								bcField.setLoading(true);
								PBS.PlusUtils.API2Request({
									url: "`+mtfScanURL+`",
									method: "GET",
									params: { type: "barcodes", changer: path },
									success: function (resp) {
										let data = resp.result.data || [];
										bcField.getStore().loadData(data.map((bc) => ({ barcode: bc })));
										bcField.setLoading(false);
									},
									failure: () => bcField.setLoading(false),
								});
							},
						},
					},
					{
						xtype: "combobox",
						itemId: "scanBarcodeField",
						name: "barcodes",
						fieldLabel: gettext("Tapes"),
						emptyText: gettext("All tapes in library"),
						editable: false,
						multiSelect: true,
						displayField: "barcode",
						valueField: "barcode",
						store: { fields: ["barcode"], data: [] },
						listConfig: { itemTpl: "{barcode}" },
						triggers: {
							clear: { cls: "fa fa-times", handler: function () { this.setValue(""); } },
						},
					},
				],
			},
			{
				xtype: "container",
				itemId: "scanDriveCt",
				layout: "anchor",
				defaults: { anchor: "100%", labelWidth: 100 },
				items: [
					{
						xtype: "combobox",
						itemId: "scanDriveField",
						name: "drive",
						fieldLabel: gettext("Drive"),
						allowBlank: false,
						editable: false,
						displayField: "name",
						valueField: "path",
						store: {
							fields: ["name", "path"],
							autoLoad: true,
							proxy: { type: "proxmox", url: "/api2/json/tape/drive", queryParam: null },
						},
					},
				],
			},
			{
				xtype: "container",
				itemId: "scanBkfCt",
				hidden: true,
				layout: "anchor",
				defaults: { anchor: "100%", labelWidth: 100 },
				items: [
					{
						xtype: "textfield",
						name: "bkf_path",
						fieldLabel: gettext(".bkf Path"),
						emptyText: "/mnt/bkf/backup.bkf",
					},
				],
			},
		],
		listeners: { destroy: () => view.getStore().rstore.load() },
	}).show();
`)

var mtfInventoryPanel = js.Panel{
	Name: "PBS.MtfManagement.InventoryPanel", XType: "pbsMtfInventoryPanel",
	Title: "MTF Inventory",
	Store: js.Store{
		StoreID: "pbs-mtf-cartridge", Model: "pbs-mtf-cartridge", Interval: 5000,
		APIPath: "/api2/extjs/config/mtf-inventory?type=cartridges",
		Sorters: "media_family_name", GroupField: "media_family_name",
	},
	Grouping: &js.Grouping{
		HeaderTemplate: js.GroupHeader("Cartridge"),
		FormatName:     js.Func("name", `return name || gettext("(unknown)");`),
	},
	ViewConfig: &js.ViewConfig{
		StripeRows: new(false),
		GetRowClass: js.Func("record", `
			if (record.get("status") === "damaged" || record.get("status") === "retired") return "proxmox-invalid-row";
			if (!record.get("catalog_type")) return "proxmox-warning-row";
			return "";
		`),
	},
	Listeners: js.Listeners{Activate: "onActivate", ItemDblClick: "showDataSets"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"startScan":  mtfScanWindow,
		"onActivate": js.Func("", `this.startStore(); this.checkScanStatus();`),
		"migrateCartridge": js.Func("", `
			let selection = this.getView().getSelection();
			if (!selection || selection.length < 1) return;
			let bc = selection[0].get("barcode");
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Create a migration job for cartridge '{0}'?"), bc),
				(btn) => {
					if (btn !== "yes") return;
					this.openJobWindow({ source_kind: "cartridge", source_ref: bc, id: "mtf-" + bc.toLowerCase() });
				},
			);
		`),
		"migrateFamily": js.Func("", `
			let selection = this.getView().getSelection();
			if (!selection || selection.length < 1) return;
			let famID = String(selection[0].get("media_family_id"));
			let famName = selection[0].get("media_family_name") || famID;
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Create a migration job for media set '{0}'?"), famName),
				(btn) => {
					if (btn !== "yes") return;
					this.openJobWindow({ source_kind: "family", source_ref: famID, id: "mtf-" + famName.toLowerCase().replace(/[^a-z0-9]+/g, "-") });
				},
			);
		`),
		"migrateDataset": js.Func("dsId", `
			Ext.Msg.confirm(
				gettext("Confirm"),
				Ext.String.format(gettext("Create a migration job for data set #{0}?"), dsId),
				(btn) => {
					if (btn !== "yes") return;
					this.openJobWindow({ source_kind: "dataset", source_ref: String(dsId), id: "mtf-ds-" + dsId });
				},
			);
		`),
		"openJobWindow": js.Func("defaults", `
			let ctrl = this;
			Ext.create("PBS.MtfManagement.JobEdit", {
				autoShow: true,
				sourceKind: defaults.source_kind,
				sourceRef: defaults.source_ref,
				defaultJobId: defaults.id,
				listeners: { destroy: () => ctrl.reload() },
			}).show();
		`),
		"showDataSets": js.Func("", `
			let view = this.getView();
			let selection = view.getSelection();
			if (!selection || selection.length < 1) return;
			let famID = selection[0].get("media_family_id");
			let famName = selection[0].get("media_family_name") || "Media Set";
			if (!famID) return;

			let ctrl = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/mtf-inventory?type=datasets&family=" + famID,
				method: "GET",
				waitMsgTarget: view,
				success: function (resp) {
					let store = Ext.create("Ext.data.Store", {
						fields: ["name", "machine_name", "owner", "write_time", "num_files", "num_directories", "id", "volumes"],
						data: resp.result.data || [],
					});
					Ext.create("Ext.window.Window", {
						title: Ext.String.format(gettext("Data Sets - {0}"), famName),
						width: 800,
						height: 400,
						layout: "fit",
						modal: true,
						items: [{
							xtype: "grid",
							store: store,
							columns: [
								{ text: "#", xtype: "rownumberer", width: 40 },
								{ text: gettext("Drive"), dataIndex: "volumes", width: 120, renderer: function (vols) {
									if (!vols || !vols.length) return "-";
									let d = vols[0].device;
									let l = vols[0].volume_label;
									return Ext.String.htmlEncode(d || "") + (l ? " (" + Ext.String.htmlEncode(l) + ")" : "");
								} },
								{ text: gettext("Machine"), dataIndex: "machine_name", flex: 1 },
								{ text: gettext("Owner"), dataIndex: "owner", flex: 1 },
								{ text: gettext("Write Time"), dataIndex: "write_time", width: 140, renderer: (v) => v ? Ext.Date.format(new Date(v * 1000), "Y-m-d H:i") : "-" },
								{ text: gettext("Files"), dataIndex: "num_files", width: 70, align: "right" },
								{ text: gettext("Dirs"), dataIndex: "num_directories", width: 70, align: "right" },
								{ xtype: "actioncolumn", width: 30, items: [{
									iconCls: "fa fa-floppy-o",
									tooltip: gettext("Migrate this data set"),
									handler: (grid, rowIdx) => ctrl.migrateDataset(store.getAt(rowIdx).get("id")),
								}] },
							],
						}],
					}).show();
				},
				failure: () => Ext.Msg.alert(gettext("Error"), gettext("Failed to load data sets.")),
			});
		`),
		"init": js.Func("view", `
			Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
			this.checkScanStatus();
			this.scanPoll = setInterval(() => this.checkScanStatus(), 5000);
			view.on("destroy", () => { if (this.scanPoll) clearInterval(this.scanPoll); });
		`),
		"checkScanStatus": js.Func("", `
			let view = this.getView();
			if (!view || view.isDestroyed) return;
			let btn = view.down("#mtfScanBtn");
			let status = view.down("#mtfScanStatus");
			PBS.PlusUtils.API2Request({
				url: "`+mtfScanURL+`",
				method: "GET",
				success: function (resp) {
					let d = resp.result.data || {};
					if (d.active) {
						if (btn) { btn.setDisabled(true); btn.setText(gettext("Scan in progress...")); }
						if (status) { status.show(); status.setHtml('<i class="fa fa-refresh fa-spin"></i> ' + gettext("An inventory scan is running.")); }
					} else {
						if (btn) { btn.setDisabled(false); btn.setText(gettext("Run Scan")); }
						if (status) { status.hide(); }
					}
				},
			});
		`),
	}},
	Tbar: []js.Tool{
		{XType: js.XButton, Text: "Reload", Handler: "reload"}, js.Sep(),
		{XType: js.XButton, Text: "Run Scan", Handler: "startScan", IconCls: "fa fa-search", ItemID: "mtfScanBtn"},
		{XType: js.XTbText, ItemID: "mtfScanStatus", Hidden: true, Cls: "proxmox-warning-row", HTML: ""},
		js.Fill(),
		{Text: "View Data Sets", Handler: "showDataSets", Disabled: true, EnableFn: js.Func("rec", `return !!rec.get("media_family_id");`)},
		{Text: "Migrate Cartridge", Handler: "migrateCartridge", Disabled: true, EnableFn: js.EnableOnRecord},
		{Text: "Migrate Set", Handler: "migrateFamily", Disabled: true, EnableFn: js.EnableOnRecord},
	},
	Columns: []js.Column{
		{Text: "Barcode", DataIndex: "barcode", Width: 140, Sortable: new(true)},
		{Text: "Label", DataIndex: "label", Flex: 1.2, Renderer: js.Func("v, meta, rec", `
			return (rec.get("is_bkf_file") ? '<i class="fa fa-file"></i> ' : "") + Ext.String.htmlEncode(v || "");
		`)},
		{Text: "Media Set", DataIndex: "media_family_name", Flex: 1.5, Sortable: new(true), Renderer: js.Func("v", `return Ext.String.htmlEncode(v || gettext("(unknown)"));`)},
		{Text: "Seq", DataIndex: "sequence", Width: 60, Align: "right"},
		{Text: "Role", DataIndex: "role", Width: 90},
		{Text: "Volumes", DataIndex: "volumes", Width: 80, Align: "right"},
		{Text: "Files", DataIndex: "files", Width: 90, Align: "right", Renderer: js.Func("v", `return v ? Ext.util.Format.number(v, "0,000") : "-";`)},
		{Text: "Last Scan", DataIndex: "last_scanned", Width: 150, Renderer: js.Func("v", `return v ? Ext.Date.format(new Date(v * 1000), "Y-m-d H:i:s") : "-";`)},
	},
}
