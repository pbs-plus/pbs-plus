Ext.define("PBS.D2DManagement.TokenPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskTokenPanel",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function () {
      let me = this;
      Ext.create("PBS.D2DManagement.TokenEditWindow", {
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      }).show();
    },
    
    onCopy: async function () {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }
			await navigator.clipboard.writeText(selection[0].data.token);
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

    render_valid: function (value) {
      if (value.toString() == "false") {
        icon = "check good";
        text = "Valid";
      } else {
        icon = "times critical";
        text = "Invalid";
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
      storeid: "proxmox-agent-tokens",
      model: "pbs-model-tokens",
      proxy: {
        type: "proxmox",
        url: pbsPlusBaseUrl + "/api2/json/d2d/token",
      },
    },
    sorters: "name",
  },

  features: [],

  tbar: [
    {
      text: gettext("Generate Token"),
      xtype: "proxmoxButton",
      handler: "onAdd",
      selModel: false,
    },
    "-",
    {
      text: gettext("Copy Token"),
      xtype: "proxmoxButton",
      handler: "onCopy",
    },
    {
      xtype: "proxmoxStdRemoveButton",
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/d2d-token",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/config/d2d-token/${encodeURIComponent(encodePathValue(rec.getId()))}`,
      callback: "reload",
    },
  ],
  columns: [
    {
      text: gettext("Token"),
      dataIndex: "token",
      flex: 1,
    },
    {
      text: gettext("Comment"),
      dataIndex: "comment",
      flex: 2,
    },
    {
      header: gettext("Validity"),
      dataIndex: "revoked",
      renderer: "render_valid",
      flex: 3,
    },
    {
      header: gettext("Created At"),
      dataIndex: "created_at",
      renderer: PBS.Utils.render_optional_timestamp,
      flex: 4,
    },
  ],
});
