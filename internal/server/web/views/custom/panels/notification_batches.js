// Notification Batch grid panel — shows all notification batches with
// add / edit / remove / status controls.

Ext.define("PBS.D2DManagement.NotificationBatchView", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsNotificationBatchView",

  title: gettext("Notification Batches"),

  stateful: true,
  stateId: "grid-notification-batches-v1",

  selType: "checkboxmodel",
  multiSelect: true,

  controller: {
    xclass: "Ext.app.ViewController",

    addBatch: function () {
      var me = this;
      Ext.create("PBS.D2DManagement.NotificationBatchEdit", {
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    editBatch: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (!selection || selection.length !== 1) return;

      Ext.create("PBS.D2DManagement.NotificationBatchEdit", {
        batchName: selection[0].data.name,
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    removeBatches: function () {
      var me = this;
      var view = me.getView();
      var recs = view.getSelection();
      if (!recs.length) return;

      Ext.Msg.confirm(
        gettext("Confirm"),
        gettext("Remove selected notification batches?"),
        function (btn) {
          if (btn !== "yes") return;
          recs.forEach(function (rec) {
            PBS.PlusUtils.API2Request({
              url:
                "/api2/json/d2d/notification-batch?batch=" +
                encodeURIComponent(rec.data.name),
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

    reload: function () {
      this.getView().getStore().load();
    },
  },

  listeners: {
    itemdblclick: "editBatch",
  },

  store: {
    fields: [
      "name",
      "comment",
      "notification-mode",
      "wait-timeout-secs",
      "send-on-timeout",
      "created-at",
      "job-count",
    ],
    autoLoad: true,
    proxy: {
      type: "ajax",
      url: pbsPlusBaseUrl + "/api2/json/d2d/notification-batch",
      reader: {
        type: "json",
        rootProperty: "data",
      },
    },
    sorters: "name",
  },

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add Batch"),
      selModel: false,
      handler: "addBatch",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit Batch"),
      handler: "editBatch",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeBatches",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
  ],

  columns: [
    {
      header: gettext("Name"),
      dataIndex: "name",
      renderer: Ext.String.htmlEncode,
      flex: 1,
      sortable: true,
    },
    {
      header: gettext("Comment"),
      dataIndex: "comment",
      renderer: Ext.String.htmlEncode,
      flex: 2,
      sortable: true,
    },
    {
      header: gettext("Notification Mode"),
      dataIndex: "notification-mode",
      width: 160,
      sortable: true,
      renderer: function (v) {
        return v === "legacy-sendmail"
          ? gettext("Legacy Sendmail")
          : gettext("Notification System");
      },
    },
    {
      header: gettext("Timeout (s)"),
      dataIndex: "wait-timeout-secs",
      width: 110,
      sortable: true,
    },
    {
      header: gettext("Send on Timeout"),
      dataIndex: "send-on-timeout",
      width: 130,
      sortable: true,
      renderer: Proxmox.Utils.format_boolean,
    },
    {
      header: gettext("Jobs"),
      dataIndex: "job-count",
      width: 70,
      sortable: true,
    },
  ],
});
