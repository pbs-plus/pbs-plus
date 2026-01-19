Ext.define("PBS.config.DiskBackupJobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskBackupJobView",

  title: "Disk Backup Jobs",

  stateful: true,
  stateId: "grid-disk-backups-v1",

  selType: "checkboxmodel", // show checkboxes
  multiSelect: true, // allow multi-row selection

  viewConfig: {
    getRowClass: function (record) {
      const lastRunEndtime = record.get("last-successful-endtime");

      if (!lastRunEndtime) {
        return "pbs-row-warning-old-backup";
      }

      const now = Date.now() / 1000;
      const sevenDaysAgo = now - 7 * 24 * 60 * 60;

      if (lastRunEndtime < sevenDaysAgo) {
        return "pbs-row-warning-old-backup";
      }

      return "";
    },
  },

  controller: {
    xclass: "Ext.app.ViewController",

    onSearchKeyUp(field) {
      const val = field.getValue().trim();
      const store = this.getView().getStore();

      // clear existing filters
      store.clearFilter(true);

      if (val) {
        // build a case-insensitive regex
        const re = new RegExp(Ext.String.escapeRegex(val), "i");
        store.filterBy((rec) => {
          // test multiple fields:
          return (
            re.test(rec.get("id")) ||
            re.test(rec.get("target")) ||
            re.test(rec.get("ns")) ||
            re.test(rec.get("comment")) ||
            re.test(rec.get("subpath"))
          );
        });
      }
    },

    runJobs: function () {
      const me = this;
      const view = me.getView();
      const recs = view.getSelection();
      if (!recs.length) return;

      const ids = recs.map((r) => r.getId());
      const list = ids.map(Ext.String.htmlEncode).join("', '");

      const msg =
        ids.length > 1
          ? Ext.String.format(gettext("Start backup jobs '{0}'?"), list)
          : Ext.String.format(gettext("Start backup job '{0}'?"), list);

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") return;

        // Build query string: job=id1&job=id2...
        const params = ids
          .map((id) => "job=" + encodeURIComponent(encodePathValue(id)))
          .join("&");

        PBS.PlusUtils.API2Request({
          url: "/api2/extjs/d2d/backup?" + params,
          method: "POST",
          waitMsgTarget: view,
          success: () => {
            me.reload();
          },
          failure: (resp) => {
            Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
          },
        });
      });
    },

    stopJobs: function () {
      const me = this;
      const view = me.getView();
      const recs = view.getSelection();
      if (!recs.length) return;

      const jobs = recs
        .map((r) => {
          const d = r.data;
          const upid = d["last-run-upid"] || "";
          const hasPlus = (d["last-run-state"] || "").startsWith("QUEUED:");
          const hasPBSTask = !!upid;
          return hasPlus || hasPBSTask
            ? { id: r.getId(), upid, hasPlus, hasPBSTask }
            : null;
        })
        .filter(Boolean);
      if (!jobs.length) return;

      const ids = jobs.map((j) => j.id);
      const list = ids.map(Ext.String.htmlEncode).join("', '");

      const msg =
        jobs.length > 1
          ? Ext.String.format(gettext("Stop backup jobs '{0}'?"), list)
          : Ext.String.format(gettext("Stop backup job '{0}'?"), list);

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") return;

        // 1) delete the "Plus" queue entry for all jobs in one request
        const plusJobs = jobs.filter((j) => j.hasPlus);
        if (plusJobs.length > 0) {
          const plusIds = plusJobs
            .map((j) => "job=" + encodeURIComponent(encodePathValue(j.id)))
            .join("&");
          PBS.PlusUtils.API2Request({
            url: "/api2/extjs/d2d/backup?" + plusIds,
            method: "DELETE",
            waitMsgTarget: view,
            success: () => {
              // Only reload if there are no PBSTasks to stop
              if (!jobs.some((j) => j.hasPBSTask)) {
                me.reload();
              }
            },
            failure: () => {
              // ignore, but still attempt PBSTask below
            },
          });
        }

        // 2) delete the PBS-side task for each job as before (no batch API for this)
        jobs.forEach((job) => {
          if (job.hasPBSTask) {
            const task = Proxmox.Utils.parse_task_upid(job.upid);
            Proxmox.Utils.API2Request({
              url:
                "/api2/extjs/nodes/" +
                task.node +
                "/tasks/" +
                encodeURIComponent(job.upid),
              method: "DELETE",
              waitMsgTarget: view,
              success: () => {
                me.reload();
              },
              failure: (resp) => {
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
              },
            });
          }
        });
      });
    },

    removeJobs: function () {
      const me = this;
      const view = me.getView();
      const recs = view.getSelection();
      if (!recs.length) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected entries?"),
        (btn) => {
          if (btn !== "yes") return;
          recs.forEach((rec) => {
            PBS.PlusUtils.API2Request({
              url:
                "/api2/extjs/config/disk-backup/" +
                encodeURIComponent(encodePathValue(rec.getId())),
              method: "DELETE",
              waitMsgTarget: view,
              failure: (resp) =>
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
              success: () => me.reload(),
            });
          });
        },
      );
    },

    addJob: function () {
      let me = this;
      Ext.create("PBS.D2DManagement.BackupJobEdit", {
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    editJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      Ext.create("PBS.D2DManagement.BackupJobEdit", {
        id: selection[0].data.id,
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    duplicateJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();

      if (!selection || selection.length < 1) {
        return;
      }

      let jobData = Ext.Object.merge({}, selection[0].data);
      delete jobData.id;

      Ext.create("PBS.D2DManagement.BackupJobEdit", {
        autoShow: true,
        jobData: jobData,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    openTaskLog: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (selection.length < 1) return;

      let upid = selection[0].data["last-run-upid"];
      if (!upid) return;

      Ext.create("PBS.plusWindow.TaskViewer", {
        upid,
      }).show();
    },

    openSuccessTaskLog: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (selection.length < 1) return;

      let upid = selection[0].data["last-successful-upid"];
      if (!upid) return;

      Ext.create("PBS.plusWindow.TaskViewer", {
        upid,
      }).show();
    },

    showLogList: function () {
      const me = this;
      const view = me.getView();
      const selection = view.getSelection();
      if (selection.length !== 1) return;

      const jobId = selection[0].getId();

      // Fetch UPIDs from API endpoint
      PBS.PlusUtils.API2Request({
        url:
          "/api2/extjs/config/disk-backup/" +
          encodeURIComponent(encodePathValue(jobId)) +
          "/upids",
        method: "GET",
        waitMsgTarget: view,
        success: function (response) {
          const upids = response.result.data || [];

          if (!upids.length) {
            Ext.Msg.alert(
              gettext("Info"),
              gettext("No task logs found for this job."),
            );
            return;
          }

          // Create a store for the UPIDs
          const upidStore = Ext.create("Ext.data.Store", {
            fields: ["upid", "starttime", "endtime", "status", "duration"],
            data: upids.map((item) => {
              const task = Proxmox.Utils.parse_task_upid(item.upid);
              const duration =
                item.endtime && task.starttime
                  ? item.endtime - task.starttime
                  : null;
              return {
                upid: item.upid,
                starttime: task.starttime,
                endtime: item.endtime,
                status: item.status,
                duration: duration,
              };
            }),
            sorters: [
              {
                property: "starttime",
                direction: "DESC",
              },
            ],
          });

          // Create a window with a grid showing all UPIDs
          Ext.create("Ext.window.Window", {
            title:
              gettext("Task Logs for Job: ") + Ext.String.htmlEncode(jobId),
            width: 900,
            height: 400,
            modal: true,
            layout: "fit",
            items: [
              {
                xtype: "grid",
                store: upidStore,
                columns: [
                  {
                    text: gettext("Start Time"),
                    dataIndex: "starttime",
                    renderer: function (value) {
                      return Proxmox.Utils.render_timestamp(value);
                    },
                    flex: 1,
                  },
                  {
                    text: gettext("End Time"),
                    dataIndex: "endtime",
                    renderer: function (value) {
                      return value
                        ? Proxmox.Utils.render_timestamp(value)
                        : "-";
                    },
                    flex: 1,
                  },
                  {
                    text: gettext("Duration"),
                    dataIndex: "duration",
                    renderer: function (value) {
                      return value !== null
                        ? Proxmox.Utils.format_duration_long(value)
                        : "-";
                    },
                    flex: 1,
                  },
                  {
                    text: gettext("Status"),
                    dataIndex: "status",
                    renderer: PBS.PlusUtils.render_task_status,
                    flex: 2,
                  },
                ],
                listeners: {
                  itemdblclick: function (grid, record) {
                    Ext.create("PBS.plusWindow.TaskViewer", {
                      upid: record.get("upid"),
                    }).show();
                  },
                },
              },
            ],
            buttons: [
              {
                text: gettext("Close"),
                handler: function () {
                  this.up("window").close();
                },
              },
            ],
          }).show();
        },
        failure: function (resp) {
          Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
        },
      });
    },

    exportCSV: async function () {
      const view = this.getView();
      const store = view.getStore();
      const records = store.getData().items.map((item) => item.data);

      if (!records || records.length === 0) {
        Ext.Msg.alert(gettext("Info"), gettext("No records to export."));
        return;
      }

      async function fetchSnapshotData(job) {
        // Build URL using job.store and job.ns.
        const url = `/api2/json/admin/datastore/${encodeURIComponent(
          job.store,
        )}/snapshots?ns=${encodeURIComponent(job.ns)}`;

        try {
          const response = await fetch(url);
          if (!response.ok) {
            throw new Error("HTTP error " + response.status);
          }
          const resData = await response.json();
          const snapshots = resData.data || [];
          let totalSize = 0;

          const backupTimes = [];

          snapshots.forEach((snap) => {
            totalSize += snap.size || 0;
            if (Object.prototype.hasOwnProperty.call(snap, "backup-time")) {
              let t = snap["backup-time"];
              if (typeof t !== "number") {
                t = parseInt(t, 10);
              }
              if (Number.isInteger(t)) {
                backupTimes.push(t);
              }
            }
          });

          return {
            snapshotCount: snapshots.length,
            snapshotTotalSize: totalSize,
            snapshotAttributes: { "backup-time": backupTimes },
          };
        } catch (error) {
          console.error("Error fetching snapshots for job:", job.id, error);
          return {
            snapshotCount: "error",
            snapshotTotalSize: "error",
            snapshotAttributes: {},
          };
        }
      }

      async function processRecords(records) {
        // Fetch snapshot data for all jobs in parallel.
        const extraDataArray = await Promise.all(
          records.map((job) => fetchSnapshotData(job)),
        );

        // Merge each job's data with the corresponding snapshot data.
        const mergedRecords = records.map((job, idx) => {
          const extra = extraDataArray[idx];

          // Process only the "backup-time" attribute.
          const backupTimes = extra.snapshotAttributes["backup-time"] || [];
          const snapshotBackupTime = JSON.stringify(
            backupTimes.map((timestamp) =>
              new Date(timestamp * 1000).toString(),
            ),
          );

          // Remove unwanted job properties.
          delete job.exclusions;
          delete job.upids;
          delete job["last-plus-error"];

          return {
            ...job,
            snapshotCount: extra.snapshotCount,
            snapshotTotalSize: extra.snapshotTotalSize,
            snapshot_backup_time: snapshotBackupTime,
          };
        });

        return mergedRecords;
      }

      // Collect the union of all keys across merged records to serve as CSV
      // headers.
      var mergedRecords = [];
      try {
        mergedRecords = await processRecords(records);
        console.log("Merged Records:", mergedRecords);
      } catch (error) {
        console.error("Error processing records:", error);
      }

      const headerSet = new Set();
      mergedRecords.forEach((record) => {
        Object.keys(record).forEach((key) => headerSet.add(key));
      });

      const headers = Array.from(headerSet);

      // Build CSV rows.
      const csvRows = [];
      csvRows.push(headers.join(","));

      mergedRecords.forEach((row) => {
        const values = headers.map((header) => {
          let val = row[header] != null ? row[header] : "";
          // Escape double quotes.
          val = String(val).replace(/"/g, '""');
          return `"${val}"`;
        });
        csvRows.push(values.join(","));
      });

      const csvText = csvRows.join("\n");

      // Create a Blob and trigger the download.
      const blob = new Blob([csvText], { type: "text/csv" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "disk-backups.csv";
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    },

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    reload: function () {
      this.getView().getStore().rstore.load();
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);

      if (!document.getElementById("pbs-backup-job-styles")) {
        const style = document.createElement("style");
        style.id = "pbs-backup-job-styles";
        style.innerHTML = `
          .pbs-row-warning-old-backup {
            background-color: #ffc107 !important;
          }
          .pbs-row-warning-old-backup .x-grid-cell {
            background-color: #ffc107 !important;
          }
          
          @media (prefers-color-scheme: dark) {
            .pbs-row-warning-old-backup,
            .pbs-row-warning-old-backup .x-grid-cell {
              background-color: rgba(255, 193, 7, 0.35) !important;
            }
          }
        `;
        document.head.appendChild(style);
      }

      // Apply custom grouper for "ns" on initialization
      const store = view.getStore();
      store.setGrouper({
        property: "ns",
        groupFn: function (record) {
          const ns = record.get("ns");
          return ns ? "Namespace: " + ns.split("/")[0] : "Namespace: /";
        },
      });
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
      storeid: "pbs-disk-backup-status",
      model: "pbs-disk-backup-status",
      interval: 5000,
    },
    sorters: "id",
    groupField: "ns",
  },

  features: [
    {
      ftype: "grouping",
      groupers: [
        {
          property: "ns",
          groupFn: function (record) {
            const ns = record.get("ns");
            return ns ? "Namespace: " + ns.split("/")[0] : "Namespace: /";
          },
        },
      ],
      groupHeaderTpl: [
        '{name:this.formatNS} ({rows.length} Item{[values.rows.length > 1 ? "s" : ""]})',
        {
          formatNS: function (ns) {
            return ns;
          },
        },
      ],
    },
  ],

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add Job"),
      selModel: false,
      handler: "addJob",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Duplicate Job"),
      handler: "duplicateJob",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return recs.length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit Job"),
      handler: "editJob",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        if (recs.length !== 1) return false;
        let d = recs[0].data;
        return !d["last-run-upid"] || !!d["last-run-state"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove Job(s)"),
      handler: "removeJobs",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        // at least one selected, and none currently running
        return (
          recs.length > 0 &&
          recs.every((r) => {
            const upid = r.data["last-run-upid"];
            const state = r.data["last-run-state"] || "";
            return !upid || !!state;
          })
        );
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log"),
      handler: "openTaskLog",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return recs.length === 1 && !!recs[0].data["last-run-upid"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show last success log"),
      handler: "openSuccessTaskLog",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return recs.length === 1 && !!recs[0].data["last-successful-upid"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log List"),
      handler: "showLogList",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return recs.length === 1;
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run Job(s)"),
      handler: "runJobs",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return (
          recs.length > 0 &&
          recs.every(
            (r) => !r.data["last-run-upid"] || !!r.data["last-run-state"],
          )
        );
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
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Export CSV"),
      handler: "exportCSV",
      selModel: false,
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
      maxWidth: 220,
      minWidth: 75,
      flex: 1,
      sortable: true,
      hidden: true,
    },
    {
      header: gettext("Target"),
      dataIndex: "target",
      width: 120,
      sortable: true,
    },
    {
      header: gettext("Subpath"),
      dataIndex: "subpath",
      width: 120,
      sortable: true,
    },
    {
      header: gettext("Datastore"),
      dataIndex: "store",
      width: 120,
      sortable: true,
      hidden: true,
    },
    {
      header: gettext("Namespace"),
      dataIndex: "ns",
      width: 120,
      sortable: true,
    },
    {
      header: gettext("Schedule"),
      dataIndex: "schedule",
      maxWidth: 220,
      minWidth: 80,
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Legacy Xattr"),
      dataIndex: "legacy-xattr",
      width: 120,
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
      header: gettext("Last Attempt"),
      dataIndex: "last-run-endtime",
      renderer: PBS.Utils.render_optional_timestamp,
      width: 140,
      sortable: true,
    },
    {
      text: gettext("Duration"),
      dataIndex: "duration",
      renderer: Proxmox.Utils.render_duration,
      width: 60,
    },
    {
      text: gettext("Read Speed"),
      dataIndex: "current_bytes_speed",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return humanReadableSpeed(value);
      },
      width: 60,
    },
    {
      text: gettext("Read Total"),
      dataIndex: "current_bytes_total",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return humanReadableBytes(value);
      },
      width: 60,
    },
    {
      text: gettext("Target Size"),
      dataIndex: "expected_size",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return humanReadableBytes(value);
      },
      width: 60,
    },
    {
      text: gettext("Processing Speed"),
      dataIndex: "current_files_speed",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return `${value.toFixed(2)} files/s`;
      },
      width: 60,
    },
    {
      text: gettext("Files Processed"),
      dataIndex: "current_file_count",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return value.toLocaleString();
      },
      width: 60,
      hidden: true,
    },
    {
      text: gettext("Folders Processed"),
      dataIndex: "current_folder_count",
      renderer: function (value) {
        if (!value && value !== 0) {
          return "-";
        }
        return value.toLocaleString();
      },
      width: 60,
      hidden: true,
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
      hidden: true,
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
