Ext.define("PBS.MtfManagement.JobView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfJobView",

  title: "MTF Migration Jobs",

  stateful: true,
  stateId: "grid-mtf-jobs-v1",



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
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      Ext.create("PBS.MtfManagement.JobEdit", {
        autoShow: true,
        jobId: selection[0].data.id,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    removeJobs: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let ids = selection.map((r) => r.getId());
      let list = ids.map(Ext.String.htmlEncode).join("', '");
      let msg = Ext.String.format(
        gettext("Delete MTF job(s) '{0}'?"),
        list,
      );

      Ext.Msg.confirm(gettext("Confirm"), msg, (btn) => {
        if (btn !== "yes") {
          return;
        }

        ids.forEach((id) => {
          PBS.PlusUtils.API2Request({
            url:
              "/api2/extjs/config/mtf-job/" +
              encodeURIComponent(encodePathValue(id)),
            method: "DELETE",
            success: () => me.reload(),
            failure: function (resp) {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        });
      });
    },

    runJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let id = selection[0].data.id;
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Start migration job '{0}'?"), id),
        function (btn) {
          if (btn !== "yes") {
            return;
          }
          PBS.PlusUtils.API2Request({
            url:
              "/api2/extjs/d2d/mtf-job?job=" +
              encodeURIComponent(encodePathValue(id)),
            method: "POST",
            waitMsgTarget: view,
            success: function () {
              me.reload();
            },
            failure: function (resp) {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        },
      );
    },

    stopJob: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let id = selection[0].data.id;
      Ext.Msg.confirm(
        gettext("Confirm"),
        Ext.String.format(gettext("Stop migration job '{0}'?"), id),
        function (btn) {
          if (btn !== "yes") {
            return;
          }
          PBS.PlusUtils.API2Request({
            url:
              "/api2/extjs/d2d/mtf-job?job=" +
              encodeURIComponent(encodePathValue(id)),
            method: "DELETE",
            waitMsgTarget: view,
            success: function () {
              me.reload();
            },
            failure: function (resp) {
              Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
            },
          });
        },
      );
    },

    openTaskLog: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      let upid = selection[0].data["last-run-upid"];
      if (!upid) {
        return;
      }
      Ext.create("PBS.plusWindow.TaskViewer", { upid }).show();
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
    },
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: "editJob",
  },

  viewConfig: {
    trackOver: false,
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
      text: gettext("Add"),
      selModel: false,
      handler: "addJob",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editJob",
      disabled: true,
      enableFn: (rec) => true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeJobs",
      disabled: true,
      enableFn: (rec) => true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Run now"),
      handler: "runJob",
      disabled: true,
      enableFn: (rec) => true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Stop"),
      handler: "stopJob",
      disabled: true,
      enableFn: function (rec) {
        if (!rec) return false;
        return rec.data["last-run-upid"] && !rec.data["last-run-state"];
      },
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Show Log"),
      handler: "openTaskLog",
      disabled: true,
      enableFn: (rec) => !!rec.data["last-run-upid"],
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
