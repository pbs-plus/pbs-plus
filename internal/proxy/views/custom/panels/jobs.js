Ext.define("PBS.config.DiskBackupJobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskBackupJobView",

  title: "Disk Backup Jobs",

  stateful: true,
  stateId: "grid-disk-backup-jobs-v1",

  controller: {
    xclass: "Ext.app.ViewController",

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

    runJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (selection.length < 1) return;

      let id = selection[0].data.id;

      Ext.create("PBS.D2DManagement.BackupWindow", {
        id,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    stopJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (selection.length < 1) return;

      let upid = selection[0].data["last-run-upid"];
      let state = selection[0].data["last-run-state"] || "";

      let hasPlusJob = false;
      let hasPBSTask = false;

      if (upid !== "") {
        hasPBSTask = true;
      }

      if (state.startsWith("QUEUED:")) {
        hasPlusJob = true;
      }

      if (!hasPBSTask && !hasPlusJob) {
        return;
      }

      let id = selection[0].data.id;

      Ext.create("PBS.D2DManagement.StopBackupWindow", {
        id,
        upid: upid,
        hasPlusJob: hasPlusJob,
        hasPBSTask: hasPBSTask,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
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
      a.download = "disk-backup-jobs.csv";
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

      // Apply custom grouper for "ns" on initialization
      const store = view.getStore();
      store.setGrouper({
        property: "ns",
        groupFn: function (record) {
          const ns = record.get("ns");
          return ns ? "Namespace: " + ns.split("/")[0] :  "Namespace: /";
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
      storeid: "pbs-disk-backup-job-status",
      model: "pbs-disk-backup-job-status",
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
            return ns ? "Namespace: " + ns.split("/")[0] :  "Namespace: /";
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
      text: gettext("Add"),
      selModel: false,
      handler: "addJob",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Duplicate"),
      handler: "duplicateJob",
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editJob",
      enableFn: (rec) =>
        !rec.data["last-run-upid"] || !!rec.data["last-run-state"],
      disabled: true,
    },
    {
      xtype: "proxmoxStdRemoveButton",
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/disk-backup-job",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/disk-backup-job/${encodeURIComponent(encodePathValue(rec.getId()))}`,
      confirmMsg: gettext("Remove entry?"),
      enableFn: (rec) =>
        !rec.data["last-run-upid"] || !!rec.data["last-run-state"],
      callback: "reload",
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log"),
      handler: "openTaskLog",
      enableFn: (rec) => !!rec.data["last-run-upid"],
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show last success log"),
      handler: "openSuccessTaskLog",
      enableFn: (rec) => !!rec.data["last-successful-upid"],
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run Job"),
      handler: "runJob",
      reference: "d2dBackupRun",
      enableFn: (rec) =>
        !rec.data["last-run-upid"] || !!rec.data["last-run-state"],
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Stop Job"),
      handler: "stopJob",
      reference: "d2dBackupStop",
      enableFn: (rec) => {
        const upid  = rec.get('last-run-upid');
        const state = rec.get('last-run-state') || "";
        return !!upid && (state === '' || state.startsWith("QUEUED:"));
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
