Ext.define("PBS.D2DManagement.PartialFilePanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskPartialFilePanel",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      let me = this;
      Ext.create("PBS.D2DManagement.PartialFileEditWindow", {
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
      Ext.create("PBS.D2DManagement.PartialFileEditWindow", {
        contentid: selection[0].data.path,
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
      storeid: "proxmox-disk-partial-files",
      model: "pbs-model-partial-files",
      proxy: {
        type: "proxmox",
        url: pbsPlusBaseUrl + "/api2/json/d2d/partial-file",
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
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/d2d-partial-file",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/config/d2d-partial-file/${encodeURIComponent(encodePathValue(rec.getId()))}`,
      callback: "reload",
    },
  ],
  columns: [
    {
      text: gettext("Path"),
      dataIndex: "path",
      flex: 1,
    },
    {
      text: gettext("Comment"),
      dataIndex: "comment",
      flex: 2,
    },
  ],
});
