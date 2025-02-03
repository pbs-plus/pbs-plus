Ext.define("PBS.D2DManagement.TargetPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskTargetPanel",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      let me = this;
      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },

    onEdit: function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      Ext.create("PBS.D2DManagement.TargetEditWindow", {
        contentid: selection[0].data.name,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    reload: function () {
      this.getView().getStore().rstore.load();
    },

    stopStore: function () {
      this.getView().getStore().rstore.stopUpdate();
    },

    startStore: function () {
      this.getView().getStore().rstore.startUpdate();
    },

    render_status: function (value) {
      if (value.toString() == "true") {
        icon = "check good";
        text = "Reachable";
      } else {
        icon = "times critical";
        text = "Unreachable";
      }

      return `<i class="fa fa-${icon}"></i> ${text}`;
    },

    init: function (view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
    },
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: "onEdit",
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "proxmox-disk-targets",
      model: "pbs-model-targets",
      proxy: {
        type: "proxmox",
        url: pbsPlusBaseUrl + "/api2/json/d2d/target",
      },
    },
    sorters: "name",
  },

  features: [],

  tbar: [
    {
      text: gettext("Add"),
      xtype: "proxmoxButton",
      handler: "onAdd",
      selModel: false,
    },
    "-",
    {
      text: gettext("Edit"),
      xtype: "proxmoxButton",
      handler: "onEdit",
      disabled: true,
    },
    {
      xtype: "proxmoxStdRemoveButton",
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/d2d-target",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/d2d-target/${encodeURIComponent(encodePathValue(rec.getId()))}`,
      callback: "reload",
    },
  ],
  columns: [
    {
      text: gettext("Name"),
      dataIndex: "name",
      flex: 1,
    },
    {
      text: gettext("Path"),
      dataIndex: "path",
      flex: 2,
    },
    {
      text: gettext("Drive Type"),
      dataIndex: "drive_type",
      flex: 1,
    },
    {
      text: gettext("Drive Name"),
      dataIndex: "drive_name",
      flex: 1,
    },
    {
      text: gettext("Drive FS"),
      dataIndex: "drive_fs",
      flex: 1,
    },
    {
      text: gettext("Drive Used"),
      dataIndex: "drive_used",
      flex: 1,
    },
    {
      header: gettext("Status"),
      dataIndex: "connection_status",
      renderer: "render_status",
      flex: 1,
    },
    {
      text: gettext("Agent Version"),
      dataIndex: "agent_version",
      flex: 1,
    },
  ],
});
