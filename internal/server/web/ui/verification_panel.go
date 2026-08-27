package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

const verificationRunURL = "/api2/extjs/d2d/verification?"

var verificationPanel = js.Panel{
	Name: "PBS.D2DVerification.JobPanel", XType: "pbsVerificationJobPanel",
	Title:       "Verification Jobs",
	MultiSelect: true, CheckboxSelection: true,
	Store: js.Store{
		StoreID: "pbs-verification-job-status", Model: "pbs-verification-job-status",
		Interval: 5000, Sorters: "id",
	},
	DockedItems: []js.Tool{
		{XType: js.XComponent, Dock: "top", Cls: "pmx-hint",
			Style: js.Obj{"padding": "8px 12px", "margin": "0 0 2px 0", "fontSize": "11px", "lineHeight": "15px"},
			HTML: "Periodically verify that backed-up files can be restored without corruption. " +
				"Each job samples files from a snapshot and checks their integrity against the source agent. " +
				"Use the results to demonstrate backup reliability over time."},
		{XType: js.XComponent, Dock: "top", Reference: "aggregateBar", Hidden: true, Cls: "x-fieldset",
			Style: js.Obj{"padding": "8px 12px", "margin": "0 0 4px 0", "fontSize": "12px", "lineHeight": "18px"},
			HTML:  "Loading..."},
	},
	Listeners: js.Listeners{ItemDblClick: "editJob"},
	Controller: js.Controller{Methods: map[string]js.Raw{
		"onSearchKeyUp":      searchFilter("id", "backup_job_id", "ns", "mode", "comment"),
		"addJob":             openEditWindow("PBS.D2DVerification.JobEdit", ""),
		"editJob":            editJobWindow("PBS.D2DVerification.JobEdit"),
		"removeJobs":         confirmRemove("/api2/extjs/config/d2d-verification/", "encodePathValue(rec.getId())", "Remove selected verification jobs?"),
		"runJobs":            runJobs(verificationRunURL, "Start verification job '{0}'?", "Start verification jobs '{0}'?"),
		"openTaskLog":        openTaskLog("last-run-upid"),
		"openSuccessTaskLog": openTaskLog("last-successful-upid"),
		"stopJobs": js.Func("", `
			const me = this;
			const view = me.getView();
			const recs = view.getSelection();
			if (!recs.length) return;
			const ids = recs.map((r) => r.getId());
			const list = ids.map(Ext.String.htmlEncode).join("', '");
			const msg = ids.length > 1
				? Ext.String.format(gettext("Stop verification jobs '{0}'?"), list)
				: Ext.String.format(gettext("Stop verification job '{0}'?"), list);
			Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
				if (btn !== "yes") return;
				PBS.PlusUtils.API2Request({
					url: "`+verificationRunURL+`" + ids.map((id) => "job=" + encodeURIComponent(encodePathValue(id))).join("&"),
					method: "DELETE",
					waitMsgTarget: view,
					success: () => me.reload(),
					failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
				});
			});
		`),
		"showResults": js.Func("", `
			const me = this;
			const view = me.getView();
			const selection = view.getSelection();
			if (!selection || selection.length !== 1) return;
			const jobId = selection[0].getId();
			PBS.PlusUtils.API2Request({
				url: "/api2/extjs/config/d2d-verification/" + encodeURIComponent(encodePathValue(jobId)) + "/results",
				method: "GET",
				waitMsgTarget: view,
				success: (response) => {
					const results = response.result.data || [];
					if (!results.length) {
						Ext.Msg.alert(gettext("Info"), gettext("No results found for this verification job."));
						return;
					}
					const renderFileStatus = (v) => {
						if (typeof v === "object" && v.status_human) return v.status_human;
						switch (v) {
							case "ok": return '<span style="color:green;">\u2713 OK</span>';
							case "failed": return '<span style="color:red;">\u2717 Failed</span>';
							case "skipped": return '<span style="opacity:0.7;">\u25CB Skipped</span>';
							case "warning": return '<span style="color:#c93;">\u26A0 Warning</span>';
							case "error": return '<span style="color:#c43;">\u26A0 Error</span>';
							default: return Ext.String.htmlEncode(v || "-");
						}
					};
					const renderSize = (bytes) => {
						if (typeof bytes === "string") return bytes || "-";
						if (!bytes && bytes !== 0) return "-";
						if (bytes < 1024) return bytes + " B";
						if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KiB";
						if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MiB";
						return (bytes / 1073741824).toFixed(2) + " GiB";
					};
					const renderPassRate = (rec) => {
						const pct = rec.get("pass_rate");
						const total = rec.get("total_files") || 0;
						const verified = rec.get("verified_files") || 0;
						const failed = rec.get("failed_files") || 0;
						if (total === 0) return "-";
						return '<span style="color:' + (failed > 0 ? "red" : "green") + ';">' +
							pct.toFixed(0) + "% (" + verified + "/" + total + ")</span>";
					};
					const runsStore = Ext.create("Ext.data.Store", {
						fields: [
							"id", "snapshot", "snapshot_time", "total_files", "total_population",
							"verified_files", "failed_files", "skipped_files", "status",
							"started_at", "completed_at", "details", "pass_rate",
							"confidence", "duration_human",
						],
						data: results,
					});
					const detailsStore = Ext.create("Ext.data.Store", {
						fields: ["path", "size", "size_human", "status", "status_human", "message"],
						data: [],
					});
					const summaryPanel = Ext.create("Ext.panel.Panel", { layout: "hbox", margin: "0 0 5 0", items: [] });
					const detailsGrid = Ext.create("Ext.grid.Panel", {
						title: gettext("File Details"),
						collapsible: true,
						collapsed: true,
						flex: 1,
						store: detailsStore,
						columns: [
							{ text: gettext("Status"), dataIndex: "status_human", width: 100, renderer: (v) => v || "-" },
							{ text: gettext("File Path"), dataIndex: "path", renderer: Ext.String.htmlEncode, flex: 3 },
							{ text: gettext("Size"), dataIndex: "size_human", width: 100, renderer: (v) => v || "-" },
							{
								text: gettext("Details"), dataIndex: "message", flex: 3,
								renderer: (v) => {
									if (!v) return "-";
									return Ext.String.htmlEncode(v).replace(
										/(agent|archive)=([0-9a-f]{8,64})/gi,
										'<span style="font-family:monospace;font-size:11px;">$1=$2</span>',
									);
								},
							},
						],
					});
					const runsGrid = Ext.create("Ext.grid.Panel", {
						title: gettext("Verification Runs"),
						flex: 1,
						store: runsStore,
						columns: [
							{
								text: gettext("Result"), dataIndex: "status_badge", width: 80,
								renderer: (v) => {
									switch (v) {
										case "passed": return '<span style="color:green;font-weight:bold;">\u2713 Passed</span>';
										case "failed": return '<span style="color:red;font-weight:bold;">\u2717 Failed</span>';
										case "warning": return '<span style="color:#c93;">\u26A0 Warning</span>';
										default: return '<span style="opacity:0.7;">' + Ext.String.htmlEncode(v || "-") + '</span>';
									}
								},
							},
							{ text: gettext("Snapshot"), dataIndex: "snapshot_human", flex: 2, renderer: Ext.String.htmlEncode },
							{ text: gettext("Pass Rate"), width: 120, renderer: (v, md, rec) => renderPassRate(rec) },
							{ text: gettext("Total"), dataIndex: "total_files", width: 60 },
							{
								text: gettext("OK"), dataIndex: "verified_files", width: 60,
								renderer: (v) => v > 0 ? '<span style="color:green;">' + v + "</span>" : v,
							},
							{
								text: gettext("Failed"), dataIndex: "failed_files", width: 60,
								renderer: (v) => v > 0 ? '<span style="color:red;"><b>' + v + "</b></span>" : v,
							},
							{ text: gettext("Skipped"), dataIndex: "skipped_files", width: 60 },
							{
								text: gettext("Started"), dataIndex: "started_at", width: 140,
								renderer: (v) => v ? Proxmox.Utils.render_timestamp(v) : "-",
							},
							{ text: gettext("Duration"), dataIndex: "duration_human", width: 90, renderer: (v) => v || "-" },
						],
						listeners: {
							selectionchange: (grid, sel) => {
								if (!sel || !sel.length) {
									detailsStore.loadData([]);
									detailsGrid.collapse();
									summaryPanel.removeAll();
									return;
								}
								const rec = sel[0];
								const details = rec.get("details") || [];
								detailsStore.loadData(details);
								const total = rec.get("total_files") || 0;
								const population = rec.get("total_population") || 0;
								const verified = rec.get("verified_files") || 0;
								const failed = rec.get("failed_files") || 0;
								const skipped = rec.get("skipped_files") || 0;
								const snap = Ext.String.htmlEncode(rec.get("snapshot_human") || rec.get("snapshot") || "");
								const conf = rec.get("confidence") || { c95: 0, c99: 0 };
								summaryPanel.removeAll();
								summaryPanel.add({
									xtype: "component",
									html:
										'<table style="width:100%;font-size:12px;">' +
										'<tr>' +
										'<td style="padding:2px 15px;"><b>Snapshot:</b> ' + snap + '</td>' +
										'<td style="padding:2px 15px;"><b>Population:</b> ' + (population || '-') + '</td>' +
										'<td style="padding:2px 15px;"><b>Sampled:</b> ' + total + '</td>' +
										'<td style="padding:2px 15px;color:green;"><b>Verified:</b> ' + verified + '</td>' +
										'<td style="padding:2px 15px;color:' + (failed > 0 ? 'red' : 'inherit') + ';"><b>Failed:</b> ' + failed + '</td>' +
										'<td style="padding:2px 15px;opacity:0.7;"><b>Skipped:</b> ' + skipped + '</td>' +
										'</tr>' +
										'<tr>' +
										'<td style="padding:2px 15px;" colspan="6">' +
										'<span><b>95% Confidence:</b> ≥' + (conf.c95 || 0).toFixed(1) + '% intact</span>' +
										'&nbsp;&nbsp;&nbsp;' +
										'<span><b>99% Confidence:</b> ≥' + (conf.c99 || 0).toFixed(1) + '% intact</span>' +
										'&nbsp;&nbsp;&nbsp;' +
										'<span style="font-weight:bold;color:' + (failed > 0 ? 'red' : 'green') + ';">' +
										(failed > 0 ? '\u2717 FAIL  -  ' + failed + ' file(s) failed verification' : '\u2713 PASS  -  all sampled files verified successfully') +
										'</span>' +
										'</td>' +
										'</tr>' +
										'</table>',
								});
								if (details.length > 0) {
									detailsGrid.expand();
								} else {
									detailsGrid.collapse();
								}
							},
						},
					});
					const descPanel = Ext.create("Ext.panel.Panel", {
						layout: "fit",
						margin: "0 0 5 0",
						items: [{
							xtype: "component",
							html:
								'<span class="pmx-hint" style="display:block;padding:4px 6px;font-size:11px;">' +
								"Each row is one verification run. Select a run to see file-level details. " +
								"The confidence values indicate the statistical lower bound on the percentage " +
								"of intact files in the snapshot, based on the sample size and results." +
								'</span>',
						}],
					});
					Ext.create("Ext.window.Window", {
						title: gettext("Verification Results: ") + Ext.String.htmlEncode(jobId),
						width: 1000,
						height: 600,
						modal: true,
						layout: { type: "vbox", align: "stretch" },
						items: [descPanel, summaryPanel, runsGrid, detailsGrid],
						buttons: [
							{
								text: gettext("Export Detail CSV"), iconCls: "fa fa-download",
								handler: () => {
									const encodedId = encodeURIComponent(encodePathValue(jobId));
									window.open(pbsPlusBaseUrl + "/api2/extjs/config/d2d-verification/" + encodedId + "/results/export?type=detail", "_blank");
								},
							},
							{
								text: gettext("Export Summary CSV"), iconCls: "fa fa-download",
								handler: () => {
									const encodedId = encodeURIComponent(encodePathValue(jobId));
									window.open(pbsPlusBaseUrl + "/api2/extjs/config/d2d-verification/" + encodedId + "/results/export?type=summary", "_blank");
								},
							},
							{ text: gettext("Close"), handler: function () { this.up("window").close(); } },
						],
					}).show();
					if (runsStore.getCount() > 0) {
						runsGrid.getSelectionModel().select(runsStore.getAt(runsStore.getCount() - 1));
					}
				},
				failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
			});
		`),
		"loadAggregate": js.Func("", `
			const me = this;
			PBS.PlusUtils.API2Request({
				url: "/api2/json/d2d/verification/aggregate",
				method: "GET",
				success: (response) => {
					const data = response.result.data;
					if (!data) return;
					const bar = me.getView().down("[reference=aggregateBar]");
					if (!bar) return;
					const totalRuns = data.total_runs || 0;
					const totalFiles = data.total_files || 0;
					const failedRuns = data.failed_runs || 0;
					const cleanRuns = data.clean_runs || 0;
					const last30 = data.last_30_days || 0;
					const passRate = data.pass_rate || 0;
					const confidence = data.confidence || 0;
					if (totalRuns === 0) {
						bar.hide();
						return;
					}
					bar.show();
					bar.setHtml(
						'<table style="width:100%;"><tr>' +
						'<td style="padding:2px 20px;"><b>Total Runs:</b> ' + totalRuns + '</td>' +
						'<td style="padding:2px 20px;"><b>Files Verified:</b> ' + totalFiles.toLocaleString() + '</td>' +
						'<td style="padding:2px 20px;color:green;"><b>Clean Runs:</b> ' + cleanRuns + ' \u2713</td>' +
						'<td style="padding:2px 20px;color:' + (failedRuns > 0 ? 'red' : 'inherit') + ';"><b>Failed Runs:</b> ' + failedRuns + (failedRuns > 0 ? ' \u2717' : '') + '</td>' +
						'</tr><tr>' +
						'<td style="padding:2px 20px;"><b>Last 30 Days:</b> ' + last30 + ' runs</td>' +
						'<td style="padding:2px 20px;"><b>Overall Pass Rate:</b> ' + passRate.toFixed(1) + '%</td>' +
						'<td style="padding:2px 20px;"><b>95% Confidence:</b> ≥' + confidence.toFixed(1) + '% intact</td>' +
						'<td style="padding:2px 20px;"><b>Jobs Configured:</b> ' + (data.total_jobs || 0) + '</td>' +
						'</tr></table>',
					);
				},
			});
		`),
		"startStore": js.Func("", `
			this.getView().getStore().rstore.startUpdate();
			this.loadAggregate();
		`),
	}},
	Tbar: []js.Tool{
		addJobTool,
		{Text: "Edit Job", Handler: "editJob", Disabled: true, EnableFn: enableOnSingleSelection},
		{Text: "Remove Job(s)", Handler: "removeJobs", Disabled: true, EnableFn: enableOnSelection}, js.Sep(),
		{Text: "Run Job(s)", Handler: "runJobs", Disabled: true, EnableFn: enableOnSelection},
		stopJobsTool,
		{Text: "Show Results", Handler: "showResults", Disabled: true, EnableFn: enableOnSingleSelection}, js.Sep(),
		showLogTool, showSuccessLogTool,
		js.Fill(), searchTool(),
	},
	Columns: []js.Column{
		{Text: "Job ID", DataIndex: "id", Flex: 1, Sortable: new(true), Renderer: "Ext.String.htmlEncode"},
		{Text: "Target", DataIndex: "backup_job_id", Width: 150, Sortable: new(true), Renderer: js.Func("v, md, rec", `
			const mode = rec.get("target_mode");
			const ns = rec.get("ns");
			if (mode === "namespace") {
				return ns && ns !== "root" ? ns : "/";
			}
			return Ext.String.htmlEncode(v || "-");
		`)},
		{Text: "Mode", DataIndex: "mode", Width: 120, Sortable: new(true), Renderer: js.Func("v", `
			switch (v) {
				case "random_spot": return "Random Spot Check";
				case "metadata": return "Metadata";
				case "full": return "Full";
				default: return v;
			}
		`)},
		{Text: "Schedule", DataIndex: "schedule", Width: 120, Sortable: new(true)},
		{Text: "Last Attempt", DataIndex: "last-run-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Last Success", DataIndex: "last-successful-endtime", Width: 140, Sortable: new(true), Renderer: "PBS.Utils.render_optional_timestamp"},
		{Text: "Last Result", Width: 90, Sortable: new(true), Renderer: js.Func("v, md, rec", `
			const state = rec.get("last-run-state") || "";
			if (state === "OK") return '<span style="color:green;font-weight:bold;">\u2713 Passed</span>';
			if (state && state.startsWith("WARN")) return '<span style="color:#c93;">\u26A0 Warning</span>';
			if (state && state !== "") return '<span style="color:red;">\u2717 Failed</span>';
			return '<span style="opacity:0.7;">-</span>';
		`)},
		{Text: "Status", DataIndex: "last-run-state", Flex: 1, Renderer: "PBS.PlusUtils.render_task_status"},
		{Text: "Next Run", DataIndex: "next-run", Width: 150, Sortable: new(true), Renderer: "PBS.Utils.render_next_task_run"},
		{Text: "Comment", DataIndex: "comment", Flex: 2, Sortable: new(true), Hidden: true, Renderer: "Ext.String.htmlEncode"},
	},
}
