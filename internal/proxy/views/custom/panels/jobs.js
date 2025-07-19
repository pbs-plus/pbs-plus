Ext.define("PBS.config.DiskBackupJobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskBackupJobView",

  title: "Disk Backup Jobs",

  stateful: true,
  stateId: "grid-disk-backup-jobs-v1",

  selType: "checkboxmodel",    // show checkboxes
  multiSelect: true,           // allow multi-row selection

  controller: {
    xclass: "Ext.app.ViewController",

    onSearchKeyUp(field) {
      const val = field.getValue().trim();
      const store = this.getView().getStore();

      // clear existing filters
      store.clearFilter(true);

      if (val) {
        // build a case-insensitive regex
        const re = new RegExp(Ext.String.escapeRegex(val), 'i');
        store.filterBy(rec => {
          // test multiple fields:
          return re.test(rec.get('id'))
            || re.test(rec.get('target'))
            || re.test(rec.get('ns'))
            || re.test(rec.get('comment'))
            || re.test(rec.get('subpath'));
        });
      }
    },

    // bulk run
    runJobs: function () {
      let me   = this;
      let recs = me.getView().getSelection();
      if (!recs || recs.length === 0) return;

      // collect all selected IDs
      let ids = Ext.Array.pluck(
        recs.map(r => r.data),
        'id',
      );

      Ext.create('PBS.D2DManagement.BackupWindow', {
        ids: ids,
        autoShow: true,
        listeners: { destroy: () => me.reload() },
      }).show();
    },

    // bulk stop
    stopJobs: function () {
      let me   = this;
      let recs = me.getView().getSelection();
      if (!recs || recs.length === 0) return;

      // build an array of job‐objects for those we can actually stop
      let jobs = [];
      Ext.Array.each(recs, (rec) => {
        const data  = rec.data;
        const upid  = data['last-run-upid'] || '';
        const state = data['last-run-state'] || '';
        const hasPlusJob = state.startsWith('QUEUED:');
        const hasPBSTask = !!upid;
        if (hasPlusJob || hasPBSTask) {
          jobs.push({
            id: data.id,
            upid,
            hasPlusJob,
            hasPBSTask,
          });
        }
      });
      if (jobs.length === 0) return;

      Ext.create('PBS.D2DManagement.StopBackupWindow', {
        jobs: jobs,
        autoShow: true,
        listeners: { destroy: () => me.reload() },
      }).show();
    },

    // bulk remove
    removeJobs: function () {
      let me = this;
      let recs = me.getView().getSelection();
      if (!recs || recs.length === 0) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected entries?"),
        function (btn) {
          if (btn !== "yes") return;
          Ext.Array.each(recs, (rec) => {
            let url = pbsPlusBaseUrl +
              "/api2/extjs/config/disk-backup-job/" +
              encodeURIComponent(encodePathValue(rec.getId()));
            PBS.Utils.API2Request({
              url,
              method: "DELETE",
              failure: (resp) => {
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
              },
            });
          });
          me.reload();
        }
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
      // only enable when exactly one row is selected
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        return sel.length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editJob",
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        if (sel.length !== 1) return false;
        let data = sel[0].data;
        return !data["last-run-upid"] || !!data["last-run-state"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxStdRemoveButton",
      text: gettext("Remove"),
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/disk-backup-job",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/disk-backup-job/${encodeURIComponent(
          encodePathValue(rec.getId()),
        )}`,
      confirmMsg: gettext("Remove entry?"),
      callback: "reload",
      // single‐select only
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        if (sel.length !== 1) return false;
        let data = sel[0].data;
        return !data["last-run-upid"] || !!data["last-run-state"];
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log"),
      handler: "openTaskLog",
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        return sel.length === 1 && !!sel[0].data["last-run-upid"];
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Show last success log"),
      handler: "openSuccessTaskLog",
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        return sel.length === 1 && !!sel[0].data["last-successful-upid"];
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run Job(s)"),
      handler: "runJobs",
      // any non-empty selection, all must be runnable
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        return (
          sel.length > 0 &&
          sel.every(
            (r) =>
              !r.data["last-run-upid"] || !!r.data["last-run-state"],
          )
        );
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Stop Job(s)"),
      handler: "stopJobs",
      enableFn: (recs) => {
        let sel = Ext.Array.from(recs);
        return (
          sel.length > 0 &&
          sel.every((r) => {
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
