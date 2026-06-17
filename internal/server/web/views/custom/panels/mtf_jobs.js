Ext.define("PBS.MtfManagement.JobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfJobView",

  title: "MTF Migration Jobs",

  stateful: true,
  stateId: "grid-mtf-jobs-v1",

  selType: "checkboxmodel",
  multiSelect: true,

  controller: {
    xclass: "Ext.app.ViewController",

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    reload: function () {
      this.getView().getStore().load();
    },

    addJob: function () {
      let me = this;
      Ext.create("PBS.MtfManagement.JobEdit", {
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
      let recs = me.getView().getSelection();
      if (!recs.length) return;
      Ext.create("PBS.MtfManagement.JobEdit", {
        autoShow: true,
        id: recs[0].data.id,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    removeJobs: function () {
      let me = this;
      let recs = me.getView().getSelection();
      if (!recs.length) return;

      let ids = recs.map((r) => r.getId());
      let list = ids.map(Ext.String.htmlEncode).join("', '");
      let msg = Ext.String.format(
        gettext("Delete MTF job(s) '{0}'?"),
        list,
      );

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") return;

        ids.forEach((id) => {
          PBS.PlusUtils.API2Request({
            url:
              "/api2/extjs/config/mtf-job/" +
              encodeURIComponent(encodePathValue(id)),
            method: "DELETE",
            success: () => me.reload(),
            failure: (resp) => {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        });
      });
    },

    runJobs: function () {
      let me = this;
      let view = me.getView();
      let recs = view.getSelection();
      if (!recs.length) return;

      let ids = recs.map((r) => r.getId());
      let params = ids
        .map((id) => "job=" + encodeURIComponent(encodePathValue(id)))
        .join("&");

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Start selected MTF migration jobs?"),
        (btn) => {
          if (btn !== "yes") return;
          PBS.PlusUtils.API2Request({
            url: "/api2/extjs/d2d/mtf-job?" + params,
            method: "POST",
            waitMsgTarget: view,
            success: () => me.reload(),
            failure: (resp) => {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        },
      );
    },

    stopJobs: function () {
      let me = this;
      let view = me.getView();
      let recs = view.getSelection();
      if (!recs.length) return;

      let ids = recs.map((r) => r.getId());
      let params = ids
        .map((id) => "job=" + encodeURIComponent(encodePathValue(id)))
        .join("&");

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Stop selected MTF migration jobs?"),
        (btn) => {
          if (btn !== "yes") return;
          PBS.PlusUtils.API2Request({
            url: "/api2/extjs/d2d/mtf-job?" + params,
            method: "DELETE",
            waitMsgTarget: view,
            success: () => me.reload(),
            failure: (resp) => {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        },
      );
    },

    openTaskLog: function () {
      let recs = this.getView().getSelection();
      if (!recs.length) return;
      let upid = recs[0].data["last-run-upid"];
      if (!upid) return;
      Ext.create("PBS.plusWindow.TaskViewer", { upid }).show();
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
      storeid: "pbs-mtf-job",
      model: "pbs-mtf-job",
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
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove Job(s)"),
      handler: "removeJobs",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return (
          recs.length > 0 &&
          recs.every((r) => !r.data["last-run-upid"] || !!r.data["last-run-state"])
        );
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run"),
      handler: "runJobs",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Stop"),
      handler: "stopJobs",
      enableFn: function () {
        let recs = this.up("grid").getSelection();
        return (
          recs.length > 0 &&
          recs.some(
            (r) =>
              r.data["last-run-upid"] &&
              !r.data["last-run-state"],
          )
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
  ],

  columns: [
    {
      header: gettext("Job ID"),
      dataIndex: "id",
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Source"),
      dataIndex: "source_label",
      flex: 1.4,
      sortable: true,
      renderer: function (v, meta, rec) {
        const kind = rec.get("source_kind") || "";
        return `<i class="fa fa-archive"></i> ${v || rec.get("source_ref")} <span style="color:#888">(${kind})</span>`;
      },
    },
    {
      header: gettext("Datastore"),
      dataIndex: "datastore",
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Namespace"),
      dataIndex: "namespace",
      flex: 1,
      sortable: true,
      renderer: function (v) {
        return v || "<span style='color:#888'>/</span>";
      },
    },
    {
      header: gettext("Schedule"),
      dataIndex: "schedule",
      width: 130,
      sortable: true,
      renderer: function (v) {
        return v || "-";
      },
    },
    {
      header: gettext("Status"),
      dataIndex: "last-run-status",
      width: 100,
      renderer: PBS.PlusUtils.render_task_status,
    },
    {
      header: gettext("Last Run"),
      dataIndex: "last-run-endtime",
      width: 150,
      renderer: function (value) {
        if (!value) return "-";
        return Ext.Date.format(new Date(value * 1000), "Y-m-d H:i:s");
      },
    },
  ],
});
