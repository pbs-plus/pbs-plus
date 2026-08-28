package management

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var alertLabelRenderer = js.Func("val", `
	var labels = {
		"stale-backup": "Stale Backup",
		"unconfigured-target": "Unconfigured Target",
		"target-offline": "Target Offline",
	};
	return labels[val] || val;
`)

var alertsPanel = js.Panel{
	Name: "PBS.D2DManagement.Alerts", XType: "pbsD2DAlertSettings",
	Title: "Alert Settings", StateID: "grid-d2d-alert-settings-v1",
	Store: js.Store{
		Fields: []js.ModelField{
			{Name: "name"}, {Name: "enabled"}, {Name: "threshold"}, {Name: "severity"}, {Name: "comment"},
			{Name: "last-sent", Type: "int"}, {Name: "cooldown-minutes", Type: "int"}, {Name: "quiet-days"},
			{Name: "skip-unscheduled", Type: "bool"}, {Name: "schedule-time"}, {Name: "schedule-window-minutes", Type: "int"},
		},
		APIPath: "/api2/json/d2d/alert-settings",
		SortBy:  []js.Sorter{{Property: "name", Direction: "ASC"}},
	},
	Listeners: js.Listeners{ItemDblClick: "editAlert"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"editAlert": js.Func("", `
			var me = this;
			var view = me.getView();
			var selection = view.getSelection();
			if (!selection || selection.length !== 1) return;
			Ext.create("PBS.D2DManagement.AlertEditWindow", {
				record: selection[0],
				autoShow: true,
				listeners: { destroy: () => me.reload() },
			});
		`),
	}},
	Tbar: []js.Tool{
		{Text: "Edit", Handler: "editAlert", Disabled: true, EnableFn: js.EnableOnSingleSelection}, js.Sep(),
		{Text: "Reload", Handler: "reload", SelModel: new(false)},
	},
	Columns: []js.Column{
		{Text: "Alert Type", DataIndex: "name", Flex: 1, Sortable: new(true), Renderer: alertLabelRenderer},
		{Text: "Enabled", DataIndex: "enabled", Width: 80, Sortable: new(true), Renderer: "Proxmox.Utils.format_boolean"},
		{Text: "Threshold", DataIndex: "threshold", Width: 100, Renderer: js.Func("val, meta, record", `
			if (record.get("name") === "stale-backup") {
				return val ? Ext.String.format("{0} days", val) : "-";
			}
			return "-";
		`)},
		{Text: "Skip Unscheduled", DataIndex: "skip-unscheduled", Width: 120, Renderer: js.Func("val, meta, record", `
			if (record.get("name") !== "stale-backup") return "-";
			return Proxmox.Utils.format_boolean(val);
		`)},
		{Text: "Cooldown", DataIndex: "cooldown-minutes", Width: 110, Renderer: js.Func("val", `
			if (!val) return "-";
			if (val < 60) return Ext.String.format("{0} min", val);
			var h = Math.floor(val / 60);
			var m = val % 60;
			if (m === 0) return Ext.String.format("{0}h", h);
			return Ext.String.format("{0}h {1}m", h, m);
		`)},
		{Text: "Quiet Days", DataIndex: "quiet-days", Width: 180, Renderer: js.Func("val", `
			if (!val || !val.length) return "-";
			return val.join(", ");
		`)},
		{Text: "Severity", DataIndex: "severity", Width: 90, Renderer: js.Func("val", `
			var colors = {
				info: "blue",
				notice: "blue",
				warning: "orange",
				error: "red",
			};
			var color = colors[val] || "black";
			return '<span style="color:' + color + ';font-weight:bold">' + val + "</span>";
		`)},
		{Text: "Schedule Time", DataIndex: "schedule-time", Width: 130, Renderer: js.Func("val, meta, record", `
			if (!val) return gettext("Any time");
			var window = record.get("schedule-window-minutes") || 60;
			var halfWindow = Math.floor(window / 2);
			return Ext.String.format("~{0} (\u00b1{1}m)", val, halfWindow);
		`)},
		{Text: "Comment", DataIndex: "comment", Flex: 1},
		{Text: "Last Sent", DataIndex: "last-sent", Width: 160, Renderer: js.Func("val", `
			if (!val) return "-";
			return new Date(val * 1000).toLocaleString();
		`)},
	},
}

var alertStaleBackupBind = js.Obj{"disabled": "{!isStaleBackup}", "visible": "{isStaleBackup}"}

var alertEditWindow = js.EditWindow{
	Name: "PBS.D2DManagement.AlertEditWindow", XType: "pbsD2DAlertEditWindow",
	Title: "Edit Alert Setting", PixelWidth: 650,
	ViewModelData: js.Obj{
		"isStaleBackup": false, "isTargetAlert": false,
		"cooldownHours": 24, "cooldownMinutes": 0,
	},
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			var me = this;
			var rec = me.initialConfig.record;
			var name = rec ? rec.get("name") : "";
			me.url = "/api2/json/d2d/alert-settings/" + encodeURIComponent(name);
			me.method = "PUT";
			me.autoLoad = !!name;
			me.alertName = name;
			me.callParent(arguments);
		`),
	},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Panel{
				Extend: js.ExtInputPanel, Reference: "alert-inputpanel", Title: "Options",
				Methods: map[string]js.Raw{
					"onSetValues": js.Func("values", `
						var panel = this;
						var vm = panel.up("pbsPlusWindowEdit").getViewModel();
						if (vm) {
							vm.set("isStaleBackup", values.name === "stale-backup");
							vm.set("isTargetAlert", values.name === "unconfigured-target" || values.name === "target-offline");
							var totalMin = values["cooldown-minutes"] || 1440;
							vm.set("cooldownHours", Math.floor(totalMin / 60));
							vm.set("cooldownMinutes", totalMin % 60);
							var scheduleTime = panel.down("timefield[reference=scheduleTime]");
							if (scheduleTime) {
								scheduleTime.setValue(values["schedule-time"] || null);
							}
							var scheduleWindow = panel.down("field[reference=scheduleWindow]");
							if (scheduleWindow) {
								scheduleWindow.setVisible(!!values["schedule-time"]);
							}
							var plusLabel = panel.down("component[reference=plusLabel]");
							if (plusLabel) {
								plusLabel.setVisible(!!values["schedule-time"]);
							}
							var minLabel = panel.down("component[reference=minLabel]");
							if (minLabel) {
								minLabel.setVisible(!!values["schedule-time"]);
							}
						}
						// Check quiet-days checkboxes after render
						var quietDays = values["quiet-days"] || [];
						Ext.defer(function () {
							var quietGroup = panel.down("checkboxgroup[reference=quietDays]");
							if (quietGroup) {
								Ext.Array.each(quietGroup.items.items, function (cb) {
									cb.setValue(Ext.Array.contains(quietDays, cb.inputValue));
								});
							}
						}, 50);
						return values;
					`),
					"onGetValues": js.Func("values", `
						var panel = this;
						var vm = panel.up("pbsPlusWindowEdit").getViewModel();
						// Build cooldown-minutes from hours + minutes
						if (vm) {
							values["cooldown-minutes"] = (vm.get("cooldownHours") || 0) * 60 + (vm.get("cooldownMinutes") || 0);
							// Clear schedule if time is empty (Any)
							if (!values["schedule-time"]) {
								values["schedule-time"] = "";
								values["schedule-window-minutes"] = "60";
							}
						}
						// Collect quiet-days from checkboxgroup, remove raw quiet-day entries
						var quietGroup = panel.down("checkboxgroup[reference=quietDays]");
						if (quietGroup) {
							var checked = [];
							Ext.Array.each(quietGroup.items.items, function (cb) {
								if (cb.checked) {
									checked.push(cb.inputValue);
								}
							});
							values["quiet-days"] = JSON.stringify(checked);
						}
						delete values["quiet-day"];
						return values;
					`),
				},
				Column1: js.Items(
					js.Field{XType: js.XDisplayField, Name: "name", Label: "Alert Type", Renderer: alertLabelRenderer, SubmitValue: true},
					js.Field{XType: js.XCheckbox, Name: "enabled", Label: "Enabled", InputValue: 1, UncheckedValue: 0, Checked: new(true)},
					js.Field{XType: js.XIntegerField, Name: "threshold", Label: "Threshold (days)", MinValue: 1, MaxValue: 365, AllowBlank: new(true), Bind: alertStaleBackupBind},
					js.Field{XType: js.XCheckbox, Name: "skip-unscheduled", Label: "Skip Unscheduled", BoxLabel: "Skip jobs without a schedule", InputValue: 1, UncheckedValue: 0, Bind: alertStaleBackupBind},
				),
				Column2: js.Items(
					js.Field{XType: js.XKVComboBox, Name: "severity", Label: "Severity", Value: "warning", ComboItems: js.Arr{
						js.Arr{"info", "Info"}, js.Arr{"notice", "Notice"}, js.Arr{"warning", "Warning"}, js.Arr{"error", "Error"},
					}},
					js.Field{XType: js.XFieldContainer, Label: "Cooldown", Layout: "hbox", Items: js.Items(
						js.Field{XType: js.XIntegerField, Reference: "cooldownHours", MinValue: 0, MaxValue: 720, Width: 70, Bind: js.Obj{"value": "{cooldownHours}"}},
						js.Field{XType: js.XDisplayField, Value: "h", Width: 25, Margins: "0 5 0 2"},
						js.Field{XType: js.XIntegerField, Reference: "cooldownMinutes", MinValue: 0, MaxValue: 59, Width: 60, Bind: js.Obj{"value": "{cooldownMinutes}"}},
						js.Field{XType: js.XDisplayField, Value: "m", Width: 25, Margins: "0 0 0 2"},
					)},
					js.Field{XType: js.XCheckboxGroup, Reference: "quietDays", Label: "Quiet Days", Columns: 4, Items: js.Items(
						js.Field{BoxLabel: "Mon", InputValue: "Monday"},
						js.Field{BoxLabel: "Tue", InputValue: "Tuesday"},
						js.Field{BoxLabel: "Wed", InputValue: "Wednesday"},
						js.Field{BoxLabel: "Thu", InputValue: "Thursday"},
						js.Field{BoxLabel: "Fri", InputValue: "Friday"},
						js.Field{BoxLabel: "Sat", InputValue: "Saturday"},
						js.Field{BoxLabel: "Sun", InputValue: "Sunday"},
					)},
					js.Field{XType: js.XFieldContainer, Label: "Alert Time", Layout: "hbox", Items: js.Items(
						js.Field{XType: js.XTimeField, Name: "schedule-time", Reference: "scheduleTime",
							Format: "H:i", SubmitFormat: "H:i", Width: 110, AllowBlank: new(true),
							EmptyText: "Any (all times)", ChangeFn: js.Func("field, val", `
								var ct = field.up("fieldcontainer");
								var win = ct.down("field[reference=scheduleWindow]");
								var plus = ct.down("component[reference=plusLabel]");
								var min = ct.down("component[reference=minLabel]");
								var hasTime = !!val;
								if (win) win.setVisible(hasTime);
								if (plus) plus.setVisible(hasTime);
								if (min) min.setVisible(hasTime);
							`)},
						js.Field{XType: js.XDisplayField, Reference: "plusLabel", Value: js.T("\u00b1"), Width: 20, Margins: "0 2 0 5", Hidden: true},
						js.Field{XType: js.XIntegerField, Name: "schedule-window-minutes", Reference: "scheduleWindow",
							MinValue: 10, MaxValue: 720, Value: 60, Width: 60, AllowBlank: new(false), Hidden: true},
						js.Field{XType: js.XDisplayField, Reference: "minLabel", Value: js.T("min window"), Width: 75, Margins: "0 0 0 2", Hidden: true},
					)},
				),
				ColumnB: js.Items(
					js.Field{XType: "proxmoxtextfield", Name: "comment", Label: "Comment", AllowBlank: new(true)},
				),
			},
			js.Panel{
				Title: "Exclusions", Layout: "fit", Reference: "exclusions-panel",
				Items: js.Items(js.Panel{
					Reference: "exclusionGrid", BorderOff: true,
					Store: js.Store{
						AutoLoad: new(false),
						Fields: []js.ModelField{
							{Name: "id", Type: "int"}, {Name: "alert-type"}, {Name: "exclude-type"},
							{Name: "exclude-value"}, {Name: "comment"},
						},
						APIPath: "/api2/json/d2d/alert-exclusions",
					},
					Tbar: []js.Tool{
						{XType: js.XButton, Text: "Add Job", IconCls: "fa fa-plus", Handler: "addJobExclusion", Bind: js.Obj{"disabled": "{isTargetAlert}"}},
						{XType: js.XButton, Text: "Add Target", IconCls: "fa fa-plus", Handler: "addTargetExclusion", Bind: js.Obj{"disabled": "{isStaleBackup}"}},
						{XType: js.XButton, Text: "Remove", IconCls: "fa fa-trash-o", Handler: "removeExclusion", Disabled: true, EnableFn: js.EnableOnSelection}, js.Sep(),
						{XType: js.XButton, Text: "Reload", IconCls: "fa fa-refresh", Handler: "reloadExclusions"},
					},
					Columns: []js.Column{
						{Text: "Type", DataIndex: "exclude-type", Width: 90, Renderer: js.Func("val", `
							return val === "job" ? "Job" : "Target";
						`)},
						{Text: "Name", DataIndex: "exclude-value", Flex: 1},
						{Text: "Comment", DataIndex: "comment", Flex: 1},
					},
				}),
			},
		),
	}),
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			var me = this;
			// Load exclusions after the window has rendered and the alert name is known
			Ext.defer(function () {
				me.loadExclusions();
			}, 200);
		`),
		"loadExclusions": js.Func("", `
			var me = this;
			var view = me.getView();
			var grid = me.lookup("exclusionGrid");
			if (!grid || !view.alertName) return;
			var store = grid.getStore();
			if (!store) return;
			store.getProxy().setUrl(
				pbsPlusBaseUrl + "/api2/json/d2d/alert-exclusions?type=" + encodeURIComponent(view.alertName),
			);
			store.load();
		`),
		"reloadExclusions": js.Func("", `this.loadExclusions();`),
		"addJobExclusion": js.Func("", `
			var me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/backup",
				method: "GET",
				success: (resp) => {
					var jobs = resp.result.data || [];
					var items = jobs.map(function (j) {
						return [j.id, "Backup: " + j.id + " (" + j.target + ")"];
					});
					me.showExclusionPicker("job", items);
				},
			});
		`),
		"addTargetExclusion": js.Func("", `
			var me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/target",
				method: "GET",
				success: (resp) => {
					var targets = resp.result.data || [];
					var items = targets.map(function (t) {
						return [t.name, t.name];
					});
					me.showExclusionPicker("target", items);
				},
			});
		`),
		"showExclusionPicker": js.Func("excludeType, items", `
			var me = this;
			var view = me.getView();
			var win = Ext.create("Ext.window.Window", {
				title: Ext.String.format("Add {0} Exclusion", excludeType === "job" ? "Job" : "Target"),
				modal: true,
				width: 400,
				layout: "fit",
				items: [{
					xtype: "form",
					bodyPadding: 10,
					items: [
						{
							xtype: "proxmoxKVComboBox",
							name: "exclude-value",
							fieldLabel: excludeType === "job" ? "Job" : "Target",
							comboItems: items,
							allowBlank: false,
							editable: false,
						},
						{
							xtype: "proxmoxtextfield",
							name: "comment",
							fieldLabel: gettext("Comment"),
							allowBlank: true,
						},
					],
				}],
				buttons: [
					{ text: gettext("Cancel"), handler: function () { win.close(); } },
					{
						text: gettext("Add"),
						handler: function () {
							var form = win.down("form");
							var vals = form.getValues();
							if (!vals["exclude-value"]) {
								return;
							}
							PBS.PlusUtils.API2Request({
								url: "/api2/json/d2d/alert-exclusions",
								method: "POST",
								params: {
									"alert-type": view.alertName,
									"exclude-type": excludeType,
									"exclude-value": vals["exclude-value"],
									comment: vals.comment || "",
								},
								success: function () {
									win.close();
									me.loadExclusions();
								},
							});
						},
					},
				],
			}).show();
		`),
		"removeExclusion": js.Func("", `
			var me = this;
			var grid = me.lookup("exclusionGrid");
			if (!grid) return;
			var selection = grid.getSelection();
			if (!selection || selection.length === 0) return;
			var exclusion = selection[0];
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/alert-exclusions/" + exclusion.get("id"),
				method: "DELETE",
				success: function () {
					me.loadExclusions();
				},
			});
		`),
	}},
}
