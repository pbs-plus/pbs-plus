Ext.define("PBS.MtfManagement.DriveGrid", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfDriveGrid",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      let me = this;
      Ext.create("PBS.TapeManagement.DriveEditWindow", {
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
      Ext.create("PBS.TapeManagement.DriveEditWindow", {
        driveid: selection[0].data.name,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    showStatus: function () {
      let view = this.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
      location.hash = `#Drive-${encodeURIComponent(selection[0].data.name)}`;
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
    itemdblclick: "showStatus",
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "proxmox-tape-drives",
      model: "pbs-model-drives",
      proxy: {
        type: "proxmox",
        url: "/api2/json/tape/drive",
        queryParam: null,
      },
    },
    sorters: "name",
    groupField: "changer",
  },

  features: [
    {
      ftype: "grouping",
      groupHeaderTpl: [
        "{name:this.formatName} ({rows.length} Drive{[values.rows.length > 1 ? \"s\" : \"\"]})",
        {
          formatName: function (changer) {
            if (!changer) {
              return gettext("Standalone Drives");
            }
            return Ext.String.format(gettext("Changer {0}"), changer);
          },
        },
      ],
    },
  ],

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
      text: gettext("Status"),
      xtype: "proxmoxButton",
      handler: "showStatus",
      disabled: true,
      iconCls: "fa fa-window-restore",
    },
    {
      xtype: "proxmoxStdRemoveButton",
      baseurl: "/api2/extjs/config/drive",
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
      text: gettext("Vendor"),
      dataIndex: "vendor",
      flex: 1,
    },
    {
      text: gettext("Model"),
      dataIndex: "model",
      flex: 1,
    },
    {
      text: gettext("Serial"),
      dataIndex: "serial",
      flex: 1,
    },
    {
      text: gettext("Drive Number"),
      dataIndex: "changer-drivenum",
      renderer: function (value, mD, record) {
        return record.data.changer ? value : "";
      },
    },
  ],
});
