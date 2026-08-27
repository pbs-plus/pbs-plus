package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var verificationHelpers = js.Raw(`
var verificationModes = Ext.create("Ext.data.Store", {
	fields: ["display", "value"],
	data: [
		{ display: "Random Spot Check", value: "random_spot" },
		// Future: { display: "Metadata Verification", value: "metadata" },
		// Future: { display: "Full Verification", value: "full" },
	],
});

var strategyDescriptions = {
	random:
		'<i class="fa fa-shuffle" style="margin-right:4px;opacity:0.6;"></i>' +
		'<b>Random</b>  -  Shuffle all eligible files and pick the first N. ' +
		"Provides statistically uniform coverage. Best for general-purpose verification.",
	systematic:
		'<i class="fa fa-arrows-left-right" style="margin-right:4px;opacity:0.6;"></i>' +
		'<b>Systematic</b>  -  Sort files by path, then pick evenly-spaced entries. ' +
		"Ensures broad spatial coverage across the entire archive.",
	stratified:
		'<i class="fa fa-layer-group" style="margin-right:4px;opacity:0.6;"></i>' +
		'<b>Stratified</b>  -  Group files by top-level directory, then sample proportionally from each group. ' +
		"Ensures every directory tree is represented.",
};

var sizeUnitStore = Ext.create("Ext.data.Store", {
	fields: ["display", "value"],
	data: [
		{ display: "B", value: 1 },
		{ display: "KiB", value: 1024 },
		{ display: "MiB", value: 1048576 },
		{ display: "GiB", value: 1073741824 },
		{ display: "TiB", value: 1099511627776 },
	],
});

function renderSizeValue(bytes) {
	if (!bytes) return "-";
	if (bytes < 1024) return bytes + " B";
	if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KiB";
	if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MiB";
	return (bytes / 1073741824).toFixed(2) + " GiB";
}

function bestUnitForBytes(bytes) {
	if (!bytes || bytes < 1024) return sizeUnitStore.getAt(0);
	for (var i = sizeUnitStore.getCount() - 1; i >= 1; i--) {
		if (bytes >= sizeUnitStore.getAt(i).get("value")) return sizeUnitStore.getAt(i);
	}
	return sizeUnitStore.getAt(0);
}
`)

var verificationFilterEditWindow = js.Panel{
	Name: "PBS.D2DVerification.FilterEditWindow", XType: "pbsD2DVerificationFilterEditWindow",
	Extend: "Ext.window.Window",
	Title:  "Add Filter", Width: 480, Modal: true, Layout: "fit", NotResizable: true,
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			var me = this;
			var isEdit = !!me.editRecord;
			if (isEdit) {
				me.title = gettext("Edit Filter");
			}
			var minBytes = isEdit ? me.editRecord.get("min_size") || 0 : 0;
			var maxBytes = isEdit ? me.editRecord.get("max_size") || 0 : 0;
			var minUnit = bestUnitForBytes(minBytes);
			var maxUnit = bestUnitForBytes(maxBytes);
			me.items = [
				{
					xtype: "form",
					reference: "filterForm",
					bodyPadding: 10,
					border: false,
					fieldDefaults: { labelWidth: 110, anchor: "100%" },
					items: [
						{
							xtype: "combo",
							name: "filter_type",
							fieldLabel: gettext("Type"),
							store: [
								["include", gettext("Include")],
								["exclude", gettext("Exclude")],
							],
							value: isEdit ? (me.editRecord.get("filter_type") || "include") : "include",
							forceSelection: true,
							editable: false,
							queryMode: "local",
						},
						{
							xtype: "textfield",
							name: "path_pattern",
							fieldLabel: gettext("Path Pattern"),
							emptyText: gettext('e.g. /data or *.log'),
							allowBlank: false,
							value: isEdit ? me.editRecord.get("path_pattern") : "",
						},
						{
							xtype: "fieldcontainer",
							fieldLabel: gettext("Min Size"),
							layout: "hbox",
							items: [
								{
									xtype: "numberfield",
									name: "min_size_val",
									minValue: 0,
									decimalPrecision: 2,
									flex: 1,
									value: minBytes > 0 ? (minBytes / minUnit.get("value")) : 0,
								},
								{
									xtype: "combo",
									name: "min_size_unit",
									store: sizeUnitStore,
									displayField: "display",
									valueField: "value",
									queryMode: "local",
									editable: false,
									anyMatch: true,
									forceSelection: true,
									value: minUnit.get("value"),
									width: 80,
									margin: "0 0 0 5",
								},
							],
						},
						{
							xtype: "fieldcontainer",
							fieldLabel: gettext("Max Size"),
							layout: "hbox",
							items: [
								{
									xtype: "numberfield",
									name: "max_size_val",
									minValue: 0,
									decimalPrecision: 2,
									flex: 1,
									value: maxBytes > 0 ? (maxBytes / maxUnit.get("value")) : 0,
								},
								{
									xtype: "combo",
									name: "max_size_unit",
									store: sizeUnitStore,
									displayField: "display",
									valueField: "value",
									queryMode: "local",
									editable: false,
									anyMatch: true,
									forceSelection: true,
									value: maxUnit.get("value"),
									width: 80,
									margin: "0 0 0 5",
								},
							],
						},
					],
					buttons: [
						{
							text: gettext("Cancel"),
							handler: function () {
								me.close();
							},
						},
						{
							text: isEdit ? gettext("Save") : gettext("Add"),
							handler: function () {
								var form = me.down("form");
								if (!form.isValid()) return;
								var vals = form.getValues();
								var minVal = parseFloat(vals.min_size_val) || 0;
								var maxVal = parseFloat(vals.max_size_val) || 0;
								var minFactor = parseInt(vals.min_size_unit, 10) || 1;
								var maxFactor = parseInt(vals.max_size_unit, 10) || 1;
								var values = {
									filter_type: vals.filter_type || "include",
									path_pattern: vals.path_pattern,
									min_size: Math.round(minVal * minFactor),
									max_size: Math.round(maxVal * maxFactor),
								};
								if (isEdit) {
									me.editRecord.set(values);
								} else {
									me.filterGrid.getStore().add(values);
								}
								me.filterGrid.syncHiddenField();
								me.close();
							},
						},
					],
				},
			];
			me.callParent();
		`),
	},
}

var verificationOptionsPanel = js.Panel{
	Name: "PBS.D2DVerification.OptionsInputPanel", XType: "pbsD2DVerificationOptionsPanel",
	Extend: js.ExtInputPanel,
	Column1: js.Items(
		js.Field{XType: js.XComponent, HTML: `'<span class="pmx-hint" style="display:block;padding:4px 6px;font-size:11px;">' +
			"Configure which backup snapshots to verify and how often to run checks. " +
			"Each run samples files from a snapshot and validates their integrity." +
			'</span>'`, Margin: "0 0 8 0"},
		js.Field{XType: js.XDisplayEditField, Name: "id", Label: "Job ID", Renderer: "Ext.htmlEncode",
			AllowBlank: new(true), EditableWhenCreate: true},
		js.Field{XType: js.XCombo, Name: "target_mode", Label: "Target Mode", QueryMode: "local",
			Store: js.Arr{js.Arr{"backup_job", js.T("Backup Job")}, js.Arr{"namespace", js.T("Namespace")}},
			Value: "backup_job", Editable: new(false), ForceSelection: true,
			AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Backup Job: verify snapshots from a single backup job. " +
				"Namespace: randomly select from all backup jobs in a datastore namespace.")},
			ChangeFn: js.Func("combo, val", `
				var panel = combo.up("pbsD2DVerificationOptionsPanel");
				if (!panel) return;
				var backupCombo = panel.down("[reference=backupJobField]");
				var nsFields = panel.down("[reference=namespaceFields]");
				if (val === "namespace") {
					if (backupCombo) { backupCombo.hide(); backupCombo.disable(); }
					if (nsFields) { nsFields.show(); nsFields.enable(); }
				} else {
					if (backupCombo) { backupCombo.show(); backupCombo.enable(); }
					if (nsFields) { nsFields.hide(); nsFields.disable(); }
				}
			`)},
		js.Field{XType: js.XCombo, Reference: "backupJobField", Label: "Backup Job", Name: "backup_job_id",
			Store:        js.Store{Fields: js.Fields("id"), APIPath: "/api2/json/d2d/backup"},
			DisplayField: "id", ValueField: "id", AllowBlank: new(false)},
		js.Field{XType: js.XFieldContainer, Reference: "namespaceFields", Layout: "vbox", Hidden: true,
			Disabled: true, Width: "100%", Items: js.Items(
				js.Field{XType: js.XDataStoreSelector, Label: "Datastore", Name: "store", Reference: "nsDatastore",
					AllowBlank: new(false), Width: "100%"},
				js.Field{XType: "pbsD2DNamespaceSelector", Label: "Namespace", Name: "ns", Reference: "nsNamespace",
					EmptyText: "Root", Width: "100%", Margin: "5 0 0 0"},
				js.Field{XType: js.XDisplayField, UserCls: "pmx-hint", Width: "100%", Padding: "5 0 0 0",
					Value: js.T("A backup job is randomly selected each run, weighted toward jobs " +
						"that have not been verified recently or have never been verified. " +
						"If a snapshot has no eligible files, the next candidate is tried.")},
				js.Field{XType: js.XPlainCheckbox, Name: "recursive", Label: "Recursive",
					BoxLabel: "Include sub-namespaces", InputValue: "true", UncheckedValue: "false", Width: "100%",
					Margin: "5 0 0 0"},
			)},
	),
	Column2: js.Items(
		js.Field{XType: "pbsD2DCalendarEvent", Label: "Schedule", Name: "schedule", EmptyText: "none (disabled)",
			DeleteEmptyWhenNotCreate: true, CBind: js.Obj{"value": "{scheduleValue}"}},
		js.Field{XType: js.XPlainCheckbox, Label: "Run after backup", Name: "run_on_backup_complete",
			InputValue: "true", UncheckedValue: "false", BoxLabel: "Wait for backup completion",
			AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Instead of running at the scheduled time, wait for the backup job to complete " +
				"successfully after the scheduled time has passed.")}},
		js.Field{XType: js.XCombo, Label: "Verification Mode", Name: "mode", QueryMode: "local",
			Store: js.Raw("verificationModes"), DisplayField: "display", ValueField: "value",
			Editable: new(false), ForceSelection: true, AllowBlank: new(false), Value: "random_spot",
			AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Random Spot Check: sample a random set of files from each snapshot and " +
				"verify their contents against the source agent. This provides statistical " +
				"confidence in backup integrity without reading every file.")}},
		js.Field{XType: "proxmoxtextfield", Label: "Number of retries", EmptyText: "0", Name: "retry"},
		js.Field{XType: "proxmoxtextfield", Label: "Retry interval (minutes)", EmptyText: "1", Name: "retry-interval"},
	),
	ColumnB: js.Items(
		js.Field{XType: "proxmoxtextfield", Label: "Comment", Name: "comment", DeleteEmptyWhenNotCreate: true},
	),
	Methods: map[string]js.Raw{
		"setValues": js.Func("values", `
			var me = this;
			me.callParent([values]);
			// Toggle UI based on target_mode
			if (values.target_mode) {
				var combo = me.down("[name=target_mode]");
				if (combo) combo.setValue(values.target_mode);
			}
			// When loading in namespace mode, set datastore on namespace selector
			if (values.target_mode === "namespace" && values.store) {
				var nsSel = me.down("[reference=nsNamespace]");
				if (nsSel && nsSel.setDatastore) {
					nsSel.setDatastore(values.store);
				}
			}
			return values;
		`),
	},
}

var verificationSpotCheckPanel = js.Panel{
	Name: "PBS.D2DVerification.SpotCheckInputPanel", XType: "pbsD2DVerificationSpotCheckPanel",
	Extend: js.ExtInputPanel,
	Column1: js.Items(
		js.Field{XType: js.XComponent, HTML: `'<span class="pmx-hint" style="display:block;padding:4px 6px;font-size:11px;">' +
			"Control how many files are checked per run and how they are selected. " +
			"More samples yield higher statistical confidence. " +
			'With 60+ samples and zero failures, confidence exceeds 95%.</span>'`, Margin: "0 0 8 0"},
		js.Field{XType: js.XCombo, Label: "Sample Mode", Name: "sample_count_mode", QueryMode: "local",
			Store: js.Arr{js.Arr{"absolute", "Absolute Count"}, js.Arr{"percent", "Percentage"}},
			Value: "absolute", Editable: new(false), ForceSelection: true,
			ChangeFn: js.Func("combo, val", `
				var panel = combo.up("pbsD2DVerificationSpotCheckPanel") || combo.up("panel");
				if (!panel) return;
				var absField = panel.down("numberfield[name=sample_count]");
				var pctField = panel.down("numberfield[name=sample_count_percent]");
				if (val === "percent") {
					if (absField) absField.disable().hide();
					if (pctField) pctField.enable().show();
				} else {
					if (absField) absField.enable().show();
					if (pctField) pctField.disable().hide();
				}
			`)},
		js.Field{XType: js.XNumberField, Label: "Sample Count", Name: "sample_count",
			MinValue: 1, MaxValue: 100000, Value: 60, AllowBlank: new(false),
			AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Number of files to verify per run. 60 samples with zero failures gives " +
				"95% confidence that at least 95% of data is intact.")}},
		js.Field{XType: js.XNumberField, Label: "Sample Percentage", Name: "sample_count_percent",
			MinValue: 0.01, MaxValue: 100, DecimalPrecision: 2, Value: 10, AllowBlank: new(false),
			Hidden: true, Disabled: true},
		js.Field{XType: js.XCombo, Label: "Sampling Strategy", Name: "sampling_strategy", QueryMode: "local",
			Store: js.Arr{js.Arr{"random", "Random"}, js.Arr{"systematic", "Systematic"}, js.Arr{"stratified", "Stratified"}},
			Value: "random", Editable: new(false), ForceSelection: true,
			ChangeFn: js.Func("combo, val", `
				var descBox = combo.up("pbsD2DVerificationSpotCheckPanel") || combo.up("panel");
				if (descBox && descBox.down("[reference=strategyDesc]")) {
					descBox.down("[reference=strategyDesc]").setHtml(strategyDescriptions[val] || "");
				}
			`)},
		js.Field{XType: js.XComponent, Reference: "strategyDesc", HTMLRaw: js.Raw(`strategyDescriptions["random"]`),
			Margin: "0 0 10 0", Cls: "x-fieldset",
			Style: js.Obj{"padding": "8px 10px", "borderRadius": "4px", "fontSize": "11px", "lineHeight": "16px"}},
		js.Field{XType: js.XPlainCheckbox, Label: "Use Latest Snapshot", Name: "use_latest",
			InputValue: "true", UncheckedValue: "false", Value: true},
		js.Field{XType: js.XFieldContainer, Label: "Date Range", Layout: "hbox", Items: js.Items(
			js.Field{XType: js.XDateField, Name: "date_from", Format: "Y-m-d", EmptyText: "From", Flex: 1},
			js.Field{XType: js.XComponent, HTML: "&nbsp;&ndash;&nbsp;"},
			js.Field{XType: js.XDateField, Name: "date_to", Format: "Y-m-d", EmptyText: "To", Flex: 1},
		)},
	),
	Column2: js.Items(
		js.Field{XType: js.XNumberField, Label: "Fail Threshold", Name: "fail_threshold",
			MinValue: 0, MaxValue: 100000, Value: 0, AllowBlank: new(true), EmptyText: "No limit",
			AutoEl: js.Obj{"tag": "div", "data-qtip": js.T("Stop verification after this many file failures. " +
				"Set to 0 to verify all sampled files regardless of failures.")}},
	),
	ColumnB: js.Items(
		js.Field{XType: js.XHiddenField, Name: "filters", Reference: "filtersHidden", Value: "[]"},
		js.Field{XType: js.XFieldSet, Title: "File Filters", Collapsible: false, Anchor: "100%",
			Padding: "5 5 0 5", Items: js.Items(
				js.Panel{
					Reference: "filterGrid", MinHeight: 120, MaxHeight: 250, Margin: "0 0 5 0",
					EmptyText:  "No filters defined  -  all files are eligible",
					ViewConfig: &js.ViewConfig{DeferEmptyText: new(false)},
					Store: js.Store{
						Fields:  js.Fields("filter_type", "path_pattern", "min_size", "max_size"),
						RawData: js.Arr{},
					},
					Columns: []js.Column{
						{Text: "Type", DataIndex: "filter_type", Width: 80, Renderer: js.Func("v", `
							if (v === "exclude") return '<span style="color:#c74b4b;">' + gettext("Exclude") + '</span>';
							return '<span style="color:#5b9b5b;">' + gettext("Include") + '</span>';
						`)},
						{Text: "Path Pattern", DataIndex: "path_pattern", Flex: 3, Renderer: "Ext.String.htmlEncode"},
						{Text: "Min Size", DataIndex: "min_size", Width: 110,
							Renderer: js.Func("v", `return v > 0 ? renderSizeValue(v) : "-";`)},
						{Text: "Max Size", DataIndex: "max_size", Width: 110,
							Renderer: js.Func("v", `return v > 0 ? renderSizeValue(v) : "-";`)},
					},
					Tbar: []js.Tool{
						{XType: js.XButton, Text: "Add", HandlerFn: js.Func("btn", `
							var grid = btn.up("grid");
							Ext.create("PBS.D2DVerification.FilterEditWindow", {
								filterGrid: grid,
							}).show();
						`)},
						{XType: js.XButton, Text: "Edit", Disabled: true, HandlerFn: js.Func("btn", `
							var grid = btn.up("grid");
							var sel = grid.getSelection();
							if (!sel.length) return;
							Ext.create("PBS.D2DVerification.FilterEditWindow", {
								filterGrid: grid,
								editRecord: sel[0],
							}).show();
						`), Render: js.Func("btn", `
							var grid = btn.up("grid");
							grid.on("selectionchange", function () {
								btn.setDisabled(grid.getSelection().length !== 1);
							});
						`)},
						js.Sep(),
						{XType: js.XButton, Text: "Remove", Disabled: true, HandlerFn: js.Func("btn", `
							var grid = btn.up("grid");
							var sel = grid.getSelection();
							if (!sel.length) return;
							grid.getStore().remove(sel);
							grid.syncHiddenField();
						`), Render: js.Func("btn", `
							var grid = btn.up("grid");
							grid.on("selectionchange", function () {
								btn.setDisabled(grid.getSelection().length === 0);
							});
						`)},
					},
					ListenersRaw: js.Obj{"boxready": js.Func("grid", `
						var panel = grid.up("pbsD2DVerificationSpotCheckPanel") || grid.up("panel");
						if (panel) {
							panel.filterGrid = grid;
							// Load any pending filters stored by setValues (handles deferred tab rendering)
							if (panel._pendingFilters) {
								grid.getStore().loadData(panel._pendingFilters);
								delete panel._pendingFilters;
							}
							// Sync hidden field and reset originalValue so dirty tracking works correctly
							var hiddenField = panel.down("hiddenfield[name=filters]");
							if (hiddenField) {
								var records = [];
								grid.getStore().each(function (rec) {
									records.push({
										filter_type: rec.get("filter_type") || "include",
										path_pattern: rec.get("path_pattern") || "",
										min_size: rec.get("min_size") || 0,
										max_size: rec.get("max_size") || 0,
									});
								});
								hiddenField.setValue(Ext.encode(records));
								// Mark current value as the baseline for dirty detection
								hiddenField.resetOriginalValue();
							}
						}
						grid.syncHiddenField = function () {
							var hidden = grid.up("pbsD2DVerificationSpotCheckPanel").down("hiddenfield[name=filters]");
							if (!hidden) return;
							var records = [];
							grid.getStore().each(function (rec) {
								records.push({
									filter_type: rec.get("filter_type") || "include",
									path_pattern: rec.get("path_pattern") || "",
									min_size: rec.get("min_size") || 0,
									max_size: rec.get("max_size") || 0,
								});
							});
							hidden.setValue(Ext.encode(records));
						};
					`)},
				},
				js.Field{XType: js.XComponent, HTMLRaw: js.Raw(`'<span class="pmx-hint" style="display:block;padding:4px 6px;font-size:11px;">' +
					gettext("Filters control which files are eligible for spot checks. " +
					"Include filters (green) allow matching files; exclude filters (red) " +
					"always block matching files. A file must match at least one include " +
					"filter and no exclude filters to be eligible. " +
					"Leave empty to sample from all files.") +
					"</span>"`)},
			)},
	),
	Methods: map[string]js.Raw{
		"setValues": js.Func("values", `
			var me = this;
			// Flatten spot_config fields into top-level values so form fields can bind
			if (values.spot_config && Ext.isObject(values.spot_config)) {
				var sc = values.spot_config;
				if (sc.sample_count !== undefined && values.sample_count === undefined) {
					values.sample_count = sc.sample_count;
				}
				if (sc.sample_count_percent !== undefined && values.sample_count_percent === undefined) {
					values.sample_count_percent = sc.sample_count_percent;
				}
				if (sc.sampling_strategy !== undefined && values.sampling_strategy === undefined) {
					values.sampling_strategy = sc.sampling_strategy;
				}
				if (sc.use_latest !== undefined && values.use_latest === undefined) {
					values.use_latest = String(sc.use_latest);
				}
				if (sc.date_from !== undefined && values.date_from === undefined) {
					values.date_from = sc.date_from;
				}
				if (sc.date_to !== undefined && values.date_to === undefined) {
					values.date_to = sc.date_to;
				}
				if (sc.fail_threshold !== undefined && values.fail_threshold === undefined) {
					values.fail_threshold = sc.fail_threshold;
				}
				if (sc.filters !== undefined && values.filters === undefined) {
					values.filters = Ext.encode(sc.filters);
				}
				// Set sample_count_mode based on which field has a value
				if (sc.sample_count_percent > 0 && values.sample_count_mode === undefined) {
					values.sample_count_mode = "percent";
				}
			}
			me.callParent([values]);
			// Parse filters JSON into the grid
			if (values.filters) {
				try {
					var filters = Ext.decode(values.filters);
					if (Ext.isArray(filters) && filters.length > 0) {
						if (me.filterGrid) {
							// Grid is already rendered  -  load directly
							me.filterGrid.getStore().loadData(filters);
							if (me.filterGrid.syncHiddenField) {
								me.filterGrid.syncHiddenField();
							}
						} else {
							// Tab not rendered yet  -  store for boxready to pick up
							me._pendingFilters = filters;
						}
					}
				} catch (e) {
					// ignore bad JSON
				}
			}
			// Update strategy description
			if (values.sampling_strategy) {
				var descBox = me.down("[reference=strategyDesc]");
				if (descBox) {
					descBox.setHtml(strategyDescriptions[values.sampling_strategy] || "");
				}
			}
			// Apply sample_count_mode toggle visibility
			var modeField = me.down("combo[name=sample_count_mode]");
			if (modeField && modeField.getValue()) {
				modeField.fireEvent("change", modeField, modeField.getValue());
			}
			return values;
		`),
		"getValues": js.Func("", `
			var vals = this.callParent(arguments);
			var mode = vals.sample_count_mode || "absolute";
			if (mode === "percent") {
				delete vals.sample_count;
			} else {
				delete vals.sample_count_percent;
			}
			delete vals.sample_count_mode;
			return vals;
		`),
	},
}

var verificationJobEdit = js.EditWindow{
	Name: "PBS.D2DVerification.JobEdit", XType: "pbsD2DVerificationJobEdit",
	Subject: "Verification Job", IsAdd: true,
	FieldDefaults: js.Obj{"labelWidth": 120},
	BodyPadding:   new(0),
	CBindData: js.Func("initialConfig", `
		var me = this;
		var baseurl = "/api2/extjs/config/d2d-verification";
		var id = initialConfig.id;
		me.isCreate = !id;
		me.url = id ? baseurl + "/" + encodeURIComponent(encodePathValue(id)) : baseurl;
		me.method = id ? "PUT" : "POST";
		me.autoLoad = !!id;
		me.scheduleValue = id ? null : "";
		me.modeValue = "random_spot";
		return {};
	`),
	Controller: js.Controller{
		Control: js.Obj{
			"pbsDataStoreSelector[name=store]": js.Obj{"change": "storeChange"},
		},
		Methods: map[string]js.Raw{
			"storeChange": js.Func("field, value", `
				var nsSelector = this.lookup("nsNamespace");
				if (nsSelector && nsSelector.setDatastore) {
					nsSelector.setDatastore(value);
				}
			`),
		},
	},
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			var me = this;
			me.callParent();
			if (me.jobData) {
				var data = Ext.apply({}, me.jobData);
				me.setValues(data);
			}
		`),
	},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Field{XType: "pbsD2DVerificationOptionsPanel", Title: "Options", CBind: js.Obj{"isCreate": "{isCreate}"}},
			js.Field{XType: "pbsD2DVerificationSpotCheckPanel", Title: "Spot Check Settings"},
			js.Raw("PBS.D2DManagement.makeNotificationTab()"),
		),
	}),
}
