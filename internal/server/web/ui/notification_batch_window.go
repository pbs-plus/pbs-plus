package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var notificationBatchEdit = js.EditWindow{
	Name: "PBS.D2DManagement.NotificationBatchEdit", XType: "pbsNotificationBatchEdit",
	Subject: "Notification Batch", IsAdd: true,
	FieldDefaults: js.Obj{"labelWidth": 140},
	BodyPadding:   new(0),
	Methods: map[string]js.Raw{
		"initComponent": js.Func("", `
			var me = this;
			var name = me.initialConfig.batchName;
			me.isCreate = !name;
			me.url = name
				? "/api2/json/d2d/notification-batch?batch=" + encodeURIComponent(name)
				: "/api2/json/d2d/notification-batch";
			me.method = name ? "PUT" : "POST";
			me.autoLoad = !!name;
			me.batchName = name || "";
			me.callParent(arguments);
		`),
	},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"init": js.Func("view", `
			var me = this;
			me.pendingAssignments = null;
			me.jobsLoaded = { backup: false, restore: false, verification: false };
			me.jobsCount = 0;
			// Load available jobs when the window opens
			me.loadAvailableJobs();
			if (view.batchName) {
				me.loadAssignedJobs(view.batchName);
			}
		`),
		"loadAvailableJobs": js.Func("", `
			var me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/backup",
				method: "GET",
				success: (resp) => {
					var jobs = (resp.result.data || []).map(function (j) {
						return { "job-type": "backup", "job-id": j.id, display: "Backup: " + j.id };
					});
					me.populateJobStore(jobs, "backup");
				},
			});
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/restore",
				method: "GET",
				success: (resp) => {
					var jobs = (resp.result.data || []).map(function (j) {
						return { "job-type": "restore", "job-id": j.id, display: "Restore: " + j.id };
					});
					me.populateJobStore(jobs, "restore");
				},
			});
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/verification",
				method: "GET",
				success: (resp) => {
					var jobs = (resp.result.data || []).map(function (j) {
						return { "job-type": "verification", "job-id": j.id, display: "Verify: " + j.id };
					});
					me.populateJobStore(jobs, "verification");
				},
			});
		`),
		"populateJobStore": js.Func("jobs, type", `
			var me = this;
			var grid = me.lookup("jobGrid");
			if (!grid) return;
			var store = grid.getStore();
			if (!store) return;
			jobs.forEach(function (j) {
				// Don't add duplicates
				if (store.findExact("job-id", j["job-id"]) < 0) {
					store.add({
						"job-type": j["job-type"],
						"job-id": j["job-id"],
						display: j.display,
						assigned: false,
					});
				}
			});
			me.jobsLoaded[type] = true;
			me.jobsCount++;
			// Once all three job types have loaded, apply pending assignments
			if (me.jobsLoaded.backup && me.jobsLoaded.restore && me.jobsLoaded.verification && me.pendingAssignments) {
				me.applyAssignments(me.pendingAssignments);
				me.pendingAssignments = null;
			}
		`),
		"loadAssignedJobs": js.Func("batchName", `
			var me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/notification-batch/jobs?batch=" + encodeURIComponent(batchName),
				method: "GET",
				success: (resp) => {
					var assigned = resp.result.data || [];
					// If all job stores are already loaded, apply immediately
					if (me.jobsLoaded.backup && me.jobsLoaded.restore && me.jobsLoaded.verification) {
						me.applyAssignments(assigned);
					} else {
						// populateJobStore will apply when all are ready
						me.pendingAssignments = assigned;
					}
				},
			});
		`),
		"applyAssignments": js.Func("assigned", `
			var me = this;
			var grid = me.lookup("jobGrid");
			if (!grid) return;
			var store = grid.getStore();
			if (!store) return;
			var sm = grid.getSelectionModel();
			var toSelect = [];
			assigned.forEach(function (a) {
				store.each(function (rec) {
					if (rec.get("job-type") === a["job-type"] && rec.get("job-id") === a["job-id"]) {
						rec.set("assigned", true);
						toSelect.push(rec);
					}
				});
			});
			if (toSelect.length > 0) {
				sm.select(toSelect, false, true);
			}
			store.commitChanges();
		`),
		"onJobSelectionChange": js.Func("sm, selected", `
			var grid = sm.view && sm.view.grid;
			if (!grid) return;
			var store = grid.getStore();
			if (!store) return;
			store.each(function (rec) {
				rec.set("assigned", false);
			});
			selected.forEach(function (rec) {
				rec.set("assigned", true);
			});
			// Mark the form dirty so the OK button enables
			var win = grid.up("pbsPlusWindowEdit");
			if (win && win.formPanel) {
				var dirtyField = win.formPanel.getForm().findField("_jobsDirty");
				if (dirtyField) {
					dirtyField.setValue(Date.now());
				}
			}
		`),
	}},
	Items: js.Items(js.Panel{
		Extend: js.ExtTabPanel, BodyPadding: 10, BorderOff: true,
		Items: js.Items(
			js.Panel{
				Extend: js.ExtInputPanel, Title: "Options",
				CBind: js.Obj{"isCreate": "{isCreate}"},
				Methods: map[string]js.Raw{
					"onGetValues": js.Func("values", `
						var panel = this;
						// Collect selected jobs from the grid
						var grid = panel.up("pbsPlusWindowEdit").down("grid[reference=jobGrid]");
						if (grid) {
							var selected = grid.getSelectionModel().getSelection();
							var jobs = selected.map(function (rec) {
								return {
									"job-type": rec.get("job-type"),
									"job-id": rec.get("job-id"),
								};
							});
							values.jobs = JSON.stringify(jobs);
						}
						// Convert checkbox value
						if (values["send-on-timeout"]) {
							values["send-on-timeout"] = "1";
						} else {
							values["send-on-timeout"] = "0";
						}
						// Remove internal dirty tracker
						delete values["_jobsDirty"];
						return values;
					`),
				},
				Column1: js.Items(
					js.Field{XType: "proxmoxtextfield", Name: "name", Label: "Batch Name", AllowBlank: new(false),
						CBind: js.Obj{"editable": "{isCreate}", "value": "{batchName}"}},
					js.Field{XType: js.XHidden, Name: "_jobsDirty", Value: 0},
				),
				Column2: js.Items(
					js.Field{XType: js.XNumberField, Name: "wait-timeout-secs", Label: "Wait Timeout (seconds)",
						MinValue: 30, MaxValue: 86400, Value: 300, AllowBlank: new(false)},
					js.Field{XType: js.XCheckbox, Name: "send-on-timeout", Label: "Send on Timeout",
						BoxLabel: "Send partial results when timeout is reached", Value: true,
						UncheckedValue: 0, InputValue: 1},
				),
				ColumnB: js.Items(
					js.Field{XType: "proxmoxtextfield", Name: "comment", Label: "Comment", DeleteEmptyWhenNotCreate: true},
				),
			},
			js.Panel{
				Title: "Jobs", Layout: "fit", MaxHeight: 300,
				Items: js.Items(js.Panel{
					Reference: "jobGrid", CheckboxSelection: true, MultiSelect: true, Scroll: true,
					Store:     js.Store{Fields: js.Fields("job-type", "job-id", "display", "assigned")},
					Listeners: js.Listeners{SelectionChange: "onJobSelectionChange"},
					Columns: []js.Column{
						{Text: "Job", DataIndex: "display", Flex: 1, Renderer: "Ext.String.htmlEncode"},
						{Text: "Type", DataIndex: "job-type", Width: 120},
					},
				}),
			},
			js.Raw("PBS.D2DManagement.makeSimpleNotificationTab()"),
		),
	}),
}
