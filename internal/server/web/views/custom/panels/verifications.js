Ext.define("PBS.D2DVerification.JobPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsVerificationJobPanel",

  title: "Verification Jobs",

  selType: "checkboxmodel",
  multiSelect: true,

  // Aggregate stats bar shown above the grid
  dockedItems: [
    {
      xtype: "component",
      dock: "top",
      cls: "pmx-hint",
      style: {
        padding: "8px 12px",
        margin: "0 0 2px 0",
        fontSize: "11px",
        lineHeight: "15px",
      },
      html:
        "Periodically verify that backed-up files can be restored without corruption. " +
        "Each job samples files from a snapshot and checks their integrity against the source agent. " +
        "Use the results to demonstrate backup reliability over time.",
    },
    {
      xtype: "component",
      dock: "top",
      reference: "aggregateBar",
      hidden: true,
      cls: "x-fieldset",
      style: {
        padding: "8px 12px",
        margin: "0 0 4px 0",
        fontSize: "12px",
        lineHeight: "18px",
      },
      html: "Loading...",
    },
  ],

  controller: {
    xclass: "Ext.app.ViewController",

    onSearchKeyUp: function (field) {
      var val = field.getValue().trim();
      var store = this.getView().getStore();
      store.clearFilter(true);
      if (val) {
        var re = new RegExp(Ext.String.escapeRegex(val), "i");
        store.filterBy(function (rec) {
          return (
            re.test(rec.get("id")) ||
            re.test(rec.get("backup_job_id")) ||
            re.test(rec.get("ns")) ||
            re.test(rec.get("mode")) ||
            re.test(rec.get("comment"))
          );
        });
      }
    },

    addJob: function () {
      var me = this;
      Ext.create("PBS.D2DVerification.JobEdit", {
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    editJob: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (!selection || selection.length < 1) return;

      Ext.create("PBS.D2DVerification.JobEdit", {
        id: selection[0].data.id,
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    removeJobs: function () {
      var me = this;
      var view = me.getView();
      var recs = view.getSelection();
      if (!recs.length) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected verification jobs?"),
        function (btn) {
          if (btn !== "yes") return;
          recs.forEach(function (rec) {
            PBS.PlusUtils.API2Request({
              url:
                "/api2/extjs/config/d2d-verification/" +
                encodeURIComponent(encodePathValue(rec.getId())),
              method: "DELETE",
              waitMsgTarget: view,
              failure: function (resp) {
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
              },
              success: function () {
                me.reload();
              },
            });
          });
        }
      );
    },

    runJobs: function () {
      var me = this;
      var view = me.getView();
      var recs = view.getSelection();
      if (!recs.length) return;

      var ids = recs.map(function (r) {
        return r.getId();
      });
      var list = ids.map(Ext.String.htmlEncode).join("', '");

      var msg =
        ids.length > 1
          ? Ext.String.format(
              gettext("Start verification jobs '{0}'?"),
              list
            )
          : Ext.String.format(
              gettext("Start verification job '{0}'?"),
              list
            );

      Ext.Msg.confirm(gettext("Confirm"), msg, function (btn) {
        if (btn !== "yes") return;

        var params = ids
          .map(function (id) {
            return "job=" + encodeURIComponent(encodePathValue(id));
          })
          .join("&");

        PBS.PlusUtils.API2Request({
          url: "/api2/extjs/d2d/verification?" + params,
          method: "POST",
          waitMsgTarget: view,
          success: function () {
            me.reload();
          },
          failure: function (resp) {
            Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
          },
        });
      });
    },

    stopJobs: function () {
      var me = this;
      var view = me.getView();
      var recs = view.getSelection();
      if (!recs.length) return;

      var ids = recs.map(function (r) {
        return r.getId();
      });
      var list = ids.map(Ext.String.htmlEncode).join("', '");

      var msg =
        ids.length > 1
          ? Ext.String.format(
              gettext("Stop verification jobs '{0}'?"),
              list
            )
          : Ext.String.format(
              gettext("Stop verification job '{0}'?"),
              list
            );

      Ext.Msg.confirm(gettext("Confirm"), msg, function (btn) {
        if (btn !== "yes") return;

        var params = ids
          .map(function (id) {
            return "job=" + encodeURIComponent(encodePathValue(id));
          })
          .join("&");

        PBS.PlusUtils.API2Request({
          url: "/api2/extjs/d2d/verification?" + params,
          method: "DELETE",
          waitMsgTarget: view,
          success: function () {
            me.reload();
          },
          failure: function (resp) {
            Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
          },
        });
      });
    },

    showResults: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (!selection || selection.length !== 1) return;

      var jobId = selection[0].getId();

      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/d2d-verification/" +
          encodeURIComponent(encodePathValue(jobId)) +
          "/results",
        method: "GET",
        waitMsgTarget: view,
        success: function (response) {
          var results = response.result.data || [];
          if (!results.length) {
            Ext.Msg.alert(
              gettext("Info"),
              gettext("No results found for this verification job.")
            );
            return;
          }

          function renderFileStatus(v) {
            // Use pre-computed status_human if available
            if (typeof v === "object" && v.status_human) return v.status_human;
            switch (v) {
              case "ok":
                return '<span style="color:green;">\u2713 OK</span>';
              case "failed":
                return '<span style="color:red;">\u2717 Failed</span>';
              case "skipped":
                return '<span style="opacity:0.7;">\u25CB Skipped</span>';
              case "warning":
                return '<span style="color:#c93;">\u26A0 Warning</span>';
              case "error":
                return '<span style="color:#c43;">\u26A0 Error</span>';
              default:
                return Ext.String.htmlEncode(v || "-");
            }
          }

          function renderSize(bytes) {
            if (typeof bytes === "string") return bytes || "-"; // already formatted
            if (!bytes && bytes !== 0) return "-";
            if (bytes < 1024) return bytes + " B";
            if (bytes < 1048576) return (bytes / 1024).toFixed(1) + " KiB";
            if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + " MiB";
            return (bytes / 1073741824).toFixed(2) + " GiB";
          }

          function renderPassRate(rec) {
            var pct = rec.get("pass_rate");
            var total = rec.get("total_files") || 0;
            var verified = rec.get("verified_files") || 0;
            var failed = rec.get("failed_files") || 0;
            if (total === 0) return "-";
            if (failed > 0)
              return (
                '<span style="color:red;">' +
                pct.toFixed(0) +
                "% (" +
                verified +
                "/" +
                total +
                ")</span>"
              );
            return (
              '<span style="color:green;">' +
              pct.toFixed(0) +
              "% (" +
              verified +
              "/" +
              total +
              ")</span>"
            );
          }

          var runsStore = Ext.create("Ext.data.Store", {
            fields: [
              "id", "snapshot", "snapshot_time", "total_files", "total_population",
              "verified_files", "failed_files", "skipped_files", "status",
              "started_at", "completed_at", "details", "pass_rate",
              "confidence", "duration_human",
            ],
            data: results,
          });

          var detailsStore = Ext.create("Ext.data.Store", {
            fields: ["path", "size", "size_human", "status", "status_human", "message"],
            data: [],
          });

          var summaryPanel = Ext.create("Ext.panel.Panel", {
            layout: "hbox",
            margin: "0 0 5 0",
            items: [],
          });

          var detailsGrid = Ext.create("Ext.grid.Panel", {
            title: gettext("File Details"),
            collapsible: true,
            collapsed: true,
            flex: 1,
            store: detailsStore,
            columns: [
              {
                text: gettext("Status"),
                dataIndex: "status_human",
                width: 100,
                renderer: function (v) {
                  return v || "-";
                },
              },
              {
                text: gettext("File Path"),
                dataIndex: "path",
                renderer: Ext.String.htmlEncode,
                flex: 3,
              },
              {
                text: gettext("Size"),
                dataIndex: "size_human",
                width: 100,
                renderer: function (v) { return v || "-"; },
              },
              {
                text: gettext("Details"),
                dataIndex: "message",
                flex: 3,
                renderer: function (v) {
                  if (!v) return "-";
                  var enc = Ext.String.htmlEncode(v);
                  enc = enc.replace(
                    /(agent|archive)=([0-9a-f]{8,64})/gi,
                    '<span style="font-family:monospace;font-size:11px;">$1=$2</span>'
                  );
                  return enc;
                },
              },
            ],
          });

          var runsGrid = Ext.create("Ext.grid.Panel", {
            title: gettext("Verification Runs"),
            flex: 1,
            store: runsStore,
            columns: [
              {
                text: gettext("Result"),
                dataIndex: "status_badge",
                width: 80,
                renderer: function (v) {
                  switch (v) {
                    case "passed":
                      return '<span style="color:green;font-weight:bold;">\u2713 Passed</span>';
                    case "failed":
                      return '<span style="color:red;font-weight:bold;">\u2717 Failed</span>';
                    case "warning":
                      return '<span style="color:#c93;">\u26A0 Warning</span>';
                    default:
                      return '<span style="opacity:0.7;">' + Ext.String.htmlEncode(v || "-") + '</span>';
                  }
                },
              },
              {
                text: gettext("Snapshot"),
                dataIndex: "snapshot_human",
                flex: 2,
                renderer: Ext.String.htmlEncode,
              },
              {
                text: gettext("Pass Rate"),
                width: 120,
                renderer: function (v, md, rec) {
                  return renderPassRate(rec);
                },
              },
              {
                text: gettext("Total"),
                dataIndex: "total_files",
                width: 60,
              },
              {
                text: gettext("OK"),
                dataIndex: "verified_files",
                width: 60,
                renderer: function (v) {
                  return v > 0
                    ? '<span style="color:green;">' + v + "</span>"
                    : v;
                },
              },
              {
                text: gettext("Failed"),
                dataIndex: "failed_files",
                width: 60,
                renderer: function (v) {
                  return v > 0
                    ? '<span style="color:red;"><b>' + v + "</b></span>"
                    : v;
                },
              },
              {
                text: gettext("Skipped"),
                dataIndex: "skipped_files",
                width: 60,
              },
              {
                text: gettext("Started"),
                dataIndex: "started_at",
                width: 140,
                renderer: function (v) {
                  return v ? Proxmox.Utils.render_timestamp(v) : "-";
                },
              },
              {
                text: gettext("Duration"),
                dataIndex: "duration_human",
                width: 90,
                renderer: function (v) { return v || "-"; },
              },
            ],
            listeners: {
              selectionchange: function (grid, sel) {
                if (!sel || !sel.length) {
                  detailsStore.loadData([]);
                  detailsGrid.collapse();
                  summaryPanel.removeAll();
                  return;
                }
                var rec = sel[0];
                var details = rec.get("details") || [];
                detailsStore.loadData(details);

                var total = rec.get("total_files") || 0;
                var population = rec.get("total_population") || 0;
                var verified = rec.get("verified_files") || 0;
                var failed = rec.get("failed_files") || 0;
                var skipped = rec.get("skipped_files") || 0;
                var snap = Ext.String.htmlEncode(rec.get("snapshot_human") || rec.get("snapshot") || "");
                var conf = rec.get("confidence") || { c95: 0, c99: 0 };

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
                    (failed > 0 ? '\u2717 FAIL — ' + failed + ' file(s) failed verification' : '\u2713 PASS — all sampled files verified successfully') +
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

          var descPanel = Ext.create("Ext.panel.Panel", {
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
            title:
              gettext("Verification Results: ") +
              Ext.String.htmlEncode(jobId),
            width: 1000,
            height: 600,
            modal: true,
            layout: {
              type: "vbox",
              align: "stretch",
            },
            items: [descPanel, summaryPanel, runsGrid, detailsGrid],
            buttons: [
              {
                text: gettext("Export Detail CSV"),
                iconCls: "fa fa-download",
                handler: function () {
                  var encodedId = encodeURIComponent(encodePathValue(jobId));
                  window.open(
                    pbsPlusBaseUrl + "/api2/extjs/config/d2d-verification/" +
                    encodedId + "/results/export?type=detail",
                    "_blank"
                  );
                },
              },
              {
                text: gettext("Export Summary CSV"),
                iconCls: "fa fa-download",
                handler: function () {
                  var encodedId = encodeURIComponent(encodePathValue(jobId));
                  window.open(
                    pbsPlusBaseUrl + "/api2/extjs/config/d2d-verification/" +
                    encodedId + "/results/export?type=summary",
                    "_blank"
                  );
                },
              },
              {
                text: gettext("Close"),
                handler: function () {
                  this.up("window").close();
                },
              },
            ],
          }).show();

          // Auto-select latest run
          if (runsStore.getCount() > 0) {
            runsGrid.getSelectionModel().select(
              runsStore.getAt(runsStore.getCount() - 1)
            );
          }
        },
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
        },
      });
    },

    openTaskLog: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (selection.length < 1) return;

      var upid = selection[0].data["last-run-upid"];
      if (!upid) return;

      Ext.create("PBS.plusWindow.TaskViewer", {
        upid: upid,
      }).show();
    },

    openSuccessTaskLog: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (selection.length < 1) return;

      var upid = selection[0].data["last-successful-upid"];
      if (!upid) return;

      Ext.create("PBS.plusWindow.TaskViewer", {
        upid: upid,
      }).show();
    },

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
      this.loadAggregate();
    },

    loadAggregate: function () {
      var me = this;
      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/verification/aggregate",
        method: "GET",
        success: function (response) {
          var data = response.result.data;
          if (!data) return;
          var bar = me.getView().down("[reference=aggregateBar]");
          if (!bar) return;

          var totalRuns = data.total_runs || 0;
          var totalFiles = data.total_files || 0;
          var totalFailed = data.total_failed || 0;
          var cleanRuns = data.clean_runs || 0;
          var failedRuns = data.failed_runs || 0;
          var last30 = data.last_30_days || 0;
          var passRate = data.pass_rate || 0;
          var confidence = data.confidence || 0;

          if (totalRuns === 0) {
            bar.hide();
            return;
          }

          bar.show();
          var html =
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
            '</tr></table>';
          bar.setHtml(html);
        },
      });
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    reload: function () {
      this.getView().getStore().rstore.load();
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
    },
  },

  listeners: {
    activate: "startStore",
    deactivate: "stopStore",
    itemdblclick: "editJob",
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "pbs-verification-job-status",
      model: "pbs-verification-job-status",
      interval: 5000,
    },
    sorters: "id",
  },

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add Job"),
      selModel: false,
      handler: "addJob",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit Job"),
      handler: "editJob",
      enableFn: function () {
        var recs = this.up("grid").getSelection();
        return recs.length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove Job(s)"),
      handler: "removeJobs",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run Job(s)"),
      handler: "runJobs",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Stop Job(s)"),
      handler: "stopJobs",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return (
          recs.length > 0 &&
          recs.every((r) => {
            const u = r.data["last-run-upid"];
            const s = r.data["last-run-state"] || "";
            return !!u && (s === "" || s.startsWith("QUEUED:"));
          })
        );
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show Results"),
      handler: "showResults",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log"),
      handler: "openTaskLog",
      enableFn: function () {
        var recs = this.up("grid").getSelection();
        return recs.length === 1 && !!recs[0].data["last-run-upid"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show last success log"),
      handler: "openSuccessTaskLog",
      enableFn: function () {
        var recs = this.up("grid").getSelection();
        return recs.length === 1 && !!recs[0].data["last-successful-upid"];
      },
      disabled: true,
    },
    "->",
    {
      xtype: "textfield",
      reference: "searchField",
      emptyText: gettext("Search..."),
      width: 200,
      enableKeyEvents: true,
      listeners: {
        keyup: { fn: "onSearchKeyUp", buffer: 300 },
      },
    },
  ],

  columns: [
    {
      header: gettext("Job ID"),
      dataIndex: "id",
      renderer: Ext.String.htmlEncode,
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Target"),
      dataIndex: "backup_job_id",
      width: 150,
      sortable: true,
      renderer: function (v, md, rec) {
        var mode = rec.get("target_mode");
        var ns = rec.get("ns");
        if (mode === "namespace") {
          return ns && ns !== "root" ? ns : "/";
        }
        return Ext.String.htmlEncode(v || "-");
      },
    },
    {
      header: gettext("Mode"),
      dataIndex: "mode",
      width: 120,
      sortable: true,
      renderer: function (v) {
        switch (v) {
          case "random_spot":
            return "Random Spot Check";
          case "metadata":
            return "Metadata";
          case "full":
            return "Full";
          default:
            return v;
        }
      },
    },
    {
      header: gettext("Schedule"),
      dataIndex: "schedule",
      width: 120,
      sortable: true,
    },
    {
      header: gettext("Last Attempt"),
      dataIndex: "last-run-endtime",
      renderer: PBS.Utils.render_optional_timestamp,
      width: 140,
      sortable: true,
    },
    {
      header: gettext("Last Success"),
      dataIndex: "last-successful-endtime",
      renderer: PBS.Utils.render_optional_timestamp,
      width: 140,
      sortable: true,
    },
    {
      header: gettext("Last Result"),
      width: 90,
      sortable: true,
      renderer: function (v, md, rec) {
        var state = rec.get("last-run-state") || "";
        if (state === "OK") return '<span style="color:green;font-weight:bold;">\u2713 Passed</span>';
        if (state && state.startsWith("WARN")) return '<span style="color:#c93;">\u26A0 Warning</span>';
        if (state && state !== "") return '<span style="color:red;">\u2717 Failed</span>';
        return '<span style="opacity:0.7;">-</span>';
      },
    },
    {
      header: gettext("Status"),
      dataIndex: "last-run-state",
      renderer: PBS.PlusUtils.render_task_status,
      flex: 1,
    },
    {
      header: gettext("Next Run"),
      dataIndex: "next-run",
      renderer: PBS.Utils.render_next_task_run,
      width: 150,
      sortable: true,
    },
    {
      header: gettext("Comment"),
      dataIndex: "comment",
      renderer: Ext.String.htmlEncode,
      flex: 2,
      sortable: true,
      hidden: true,
    },
  ],
});
