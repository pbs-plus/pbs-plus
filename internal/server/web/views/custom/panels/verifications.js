Ext.define("PBS.D2DVerification.JobPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsVerificationJobPanel",

  title: "Verification Jobs",

  selType: "checkboxmodel",
  multiSelect: true,

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

          var store = Ext.create("Ext.data.Store", {
            fields: [
              "id",
              "snapshot",
              "snapshot_time",
              "total_files",
              "verified_files",
              "failed_files",
              "skipped_files",
              "status",
              "started_at",
              "completed_at",
            ],
            data: results,
          });

          Ext.create("Ext.window.Window", {
            title:
              gettext("Verification Results: ") +
              Ext.String.htmlEncode(jobId),
            width: 900,
            height: 400,
            modal: true,
            layout: "fit",
            items: [
              {
                xtype: "grid",
                store: store,
                columns: [
                  {
                    text: gettext("Snapshot"),
                    dataIndex: "snapshot",
                    flex: 2,
                  },
                  {
                    text: gettext("Total"),
                    dataIndex: "total_files",
                    width: 70,
                  },
                  {
                    text: gettext("Verified"),
                    dataIndex: "verified_files",
                    width: 80,
                    renderer: function (v) {
                      return v > 0
                        ? '<span style="color:green;">' + v + "</span>"
                        : v;
                    },
                  },
                  {
                    text: gettext("Failed"),
                    dataIndex: "failed_files",
                    width: 70,
                    renderer: function (v) {
                      return v > 0
                        ? '<span style="color:red;">' + v + "</span>"
                        : v;
                    },
                  },
                  {
                    text: gettext("Skipped"),
                    dataIndex: "skipped_files",
                    width: 70,
                  },
                  {
                    text: gettext("Status"),
                    dataIndex: "status",
                    flex: 1,
                  },
                  {
                    text: gettext("Started"),
                    dataIndex: "started_at",
                    renderer: function (v) {
                      return v ? Proxmox.Utils.render_timestamp(v) : "-";
                    },
                    flex: 1,
                  },
                ],
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
      text: gettext("Show Results"),
      handler: "showResults",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
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
      header: gettext("Backup Job"),
      dataIndex: "backup_job_id",
      width: 150,
      sortable: true,
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
