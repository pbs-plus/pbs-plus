Ext.define("PBS.config.DiskRestoreJobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskRestoreJobView",

  title: "Disk Restore Jobs",

  stateful: true,
  stateId: "grid-disk-restore-jobs-v1",

  selType: "checkboxmodel", // show checkboxes
  multiSelect: true, // allow multi-row selection

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
            re.test(rec.get("ns")) ||
            re.test(rec.get("snapshot")) ||
            re.test(rec.get("src-path")) ||
            re.test(rec.get("dest-target")) ||
            re.test(rec.get("comment")) ||
            re.test(rec.get("dest-path"))
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
          ? Ext.String.format(gettext("Start restore jobs '{0}'?"), list)
          : Ext.String.format(gettext("Start restore job '{0}'?"), list);

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") return;

        // Build query string: job=id1&job=id2...
        const params = ids
          .map((id) => "job=" + encodeURIComponent(encodePathValue(id)))
          .join("&");

        PBS.PlusUtils.API2Request({
          url: "/api2/extjs/d2d/restore?" + params,
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
          ? Ext.String.format(gettext("Stop restore jobs '{0}'?"), list)
          : Ext.String.format(gettext("Stop restore job '{0}'?"), list);

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") return;

        // 1) delete the “Plus” queue entry for all jobs in one request
        const plusJobs = jobs.filter((j) => j.hasPlus);
        if (plusJobs.length > 0) {
          const plusIds = plusJobs
            .map((j) => "job=" + encodeURIComponent(encodePathValue(j.id)))
            .join("&");
          PBS.PlusUtils.API2Request({
            url: "/api2/extjs/d2d/restore?" + plusIds,
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
                "/api2/extjs/config/disk-restore-job/" +
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
      Ext.create("PBS.D2DManagement.RestoreJobEdit", {
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

      Ext.create("PBS.D2DManagement.RestoreJobEdit", {
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

      Ext.create("PBS.D2DManagement.RestoreJobEdit", {
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

      // Apply custom grouper for "dest-target" on initialization
      const store = view.getStore();
      store.setGrouper({
        property: "dest-target",
        groupFn: function (record) {
          const target = record.get("dest-target");
          return target ? "Target: " + target : "Target: N/A";
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
      storeid: "pbs-disk-restore-job-status",
      model: "pbs-disk-restore-job-status",
      interval: 5000,
    },
    sorters: "id",
    groupField: "dest-target",
  },

  features: [
    {
      ftype: "grouping",
      groupers: [
        {
          property: "dest-target",
          groupFn: function (record) {
            const target = record.get("dest-target");
            return target ? "Target: " + target : "Target: N/A";
          },
        },
      ],
      groupHeaderTpl: [
        '{name:this.formatTarget} ({rows.length} Item{[values.rows.length > 1 ? "s" : ""]})',
        {
          formatTarget: function (target) {
            return target;
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
      header: gettext("Snapshot"),
      dataIndex: "snapshot",
      width: 120,
      sortable: true,
    },
    {
      header: gettext("Target Destination"),
      dataIndex: "dest-target",
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
      header: gettext("Datastore"),
      dataIndex: "store",
      width: 120,
      sortable: true,
      hidden: true,
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
      header: gettext("Comment"),
      dataIndex: "comment",
      renderer: Ext.String.htmlEncode,
      flex: 2,
      sortable: true,
      hidden: true,
    },
  ],
});
