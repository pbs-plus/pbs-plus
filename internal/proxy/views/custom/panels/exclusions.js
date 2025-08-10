Ext.define("PBS.D2DManagement.ExclusionPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskExclusionPanel",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function() {
      let me = this;
      Ext.create("PBS.D2DManagement.ExclusionEditWindow", {
        listeners: {
          destroy: function() {
            me.reload();
          },
        },
      }).show();
    },

    onEdit: function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      Ext.create("PBS.D2DManagement.ExclusionEditWindow", {
        contentid: selection[0].data.path,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    reload: function() {
      this.getView().getStore().rstore.load();
    },

    stopStore: function() {
      this.getView().getStore().rstore.stopUpdate();
    },

    startStore: function() {
      this.getView().getStore().rstore.startUpdate();
    },

    init: function(view) {
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
      storeid: "proxmox-disk-exclusions",
      model: "pbs-model-exclusions",
      proxy: {
        type: "pbsplus",
        url: pbsPlusBaseUrl + "/api2/json/d2d/exclusion",
        withCredentials: true,
        cors: true,
        useDefaultXhrHeader: false,
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
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/d2d-exclusion",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/d2d-exclusion/${encodeURIComponent(encodePathValue(rec.getId()))}`,
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
