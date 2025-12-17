Ext.define("PBS.D2DManagement.ScriptPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskScriptPanel",

  stateful: true,
  stateId: "grid-disk-backup-scripts-v1",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function() {
      let me = this;
      Ext.create("PBS.D2DManagement.ScriptEditWindow", {
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
      Ext.create("PBS.D2DManagement.ScriptEditWindow", {
        contentid: selection[0].data.path,
        autoLoad: true,
        listeners: {
          destroy: () => me.reload(),
        },
      }).show();
    },

    removeScripts: function() {
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
                "/api2/extjs/config/d2d-script/" +
                encodeURIComponent(encodePathValue(rec.getId())),
              method: "DELETE",
              waitMsgTarget: view,
              failure: (resp) =>
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
              success: () => me.reload(),
            });
          });
        }
      );
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
      storeid: "proxmox-disk-scripts",
      model: "pbs-model-scripts",
      proxy: {
        type: "pbsplus",
        url: pbsPlusBaseUrl + "/api2/json/d2d/script",
      },
    },
    sorters: "path",
  },

  features: [
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
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeScripts",
      enableFn: function() {
        let recs = this.up("grid").getSelection();
        return recs.length > 0;
      },
      disabled: true,
    },
  ],
  columns: [
    {
      text: gettext("Path"),
      dataIndex: "path",
      flex: 2,
    },
    {
      text: gettext("Description"),
      dataIndex: "description",
      flex: 1,
    },
    {
      text: gettext("Job Count"),
      dataIndex: "job_count",
      flex: 1,
    },
    {
      text: gettext("Target Count"),
      dataIndex: "target_count",
      flex: 1,
    },
  ],
});
