Ext.define("PBS.config.DiskBackupJobView", {
  extend: "Ext.grid.GridPanel",
  alias: "widget.pbsDiskBackupJobView",

  stateful: true,
  stateId: "grid-disk-backup-jobs-v1",

  title: "Disk Backup Jobs",

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
          job.store
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
          records.map((job) => fetchSnapshotData(job))
        );

        // Merge each job's data with the corresponding snapshot data.
        const mergedRecords = records.map((job, idx) => {
          const extra = extraDataArray[idx];

          // Process only the "backup-time" attribute.
          const backupTimes = extra.snapshotAttributes["backup-time"] || [];
          const snapshotBackupTime = JSON.stringify(
            backupTimes.map((timestamp) => new Date(timestamp * 1000).toString())
          );

          // Remove unwanted job properties.
          delete job.exclusions;
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
      var mergedRecords = []
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
    },
  },

  listeners: {
    activate: "startStore",
    deactivate: "stopStore",
    itemdblclick: "editJob",
  },

  store: {
    type: "diff",
    autoDestroy: true,
    autoDestroyRstore: true,
    sorters: "id",
    rstore: {
      type: "update",
      storeid: "pbs-disk-backup-job-status",
      model: "pbs-disk-backup-job-status",
      interval: 5000,
    },
  },

  viewConfig: {
    trackOver: false,
  },

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add"),
      selModel: false,
      handler: "addJob",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editJob",
      disabled: true,
    },
    {
      xtype: "proxmoxStdRemoveButton",
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/disk-backup-job",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/disk-backup-job/${encodeURIComponent(encodePathValue(rec.getId()))}`,
      confirmMsg: gettext("Remove entry?"),
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
      text: gettext("Run now"),
      handler: "runJob",
      reference: "d2dBackupRun",
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
      header: gettext("Last Backup Attempt"),
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
      dataIndex: "current_read_speed",
      renderer: function(value) {
	      if (value === "") {
	        return '-';
	      }
	      return value;
      },
      width: 60,
    },
    {
      text: gettext("Write Speed"),
      dataIndex: "current_write_speed",
      renderer: function(value) {
	      if (value === "") {
	        return '-';
	      }
	      return value;
      },
      width: 60,
    },
    {
      text: gettext("Read Total"),
      dataIndex: "current_read_total",
      renderer: function(value) {
	      if (value === "") {
	        return '-';
	      }
	      return value;
      },
      width: 60,
    },
    {
      text: gettext("Write Total"),
      dataIndex: "current_write_total",
      renderer: function(value) {
	      if (value === "") {
	        return '-';
	      }
	      return value;
      },
      width: 60,
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
    },
  ],

  initComponent: function () {
    let me = this;

    me.callParent();
  },
});
