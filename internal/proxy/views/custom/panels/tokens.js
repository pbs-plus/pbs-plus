Ext.define("PBS.D2DManagement.TokenPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsDiskTokenPanel",

  controller: {
    xclass: "Ext.app.ViewController",

    onAdd: function() {
      let me = this;
      Ext.create("PBS.D2DManagement.TokenEditWindow", {
        listeners: {
          destroy: function() {
            me.reload();
          },
        },
      }).show();
    },

    onCopy: async function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let token = selection[0].data.token;
      Ext.create("Ext.window.Window", {
        modal: true,
        width: 600,
        title: gettext("Bootstrap Token"),
        layout: "form",
        bodyPadding: "10 0",
        items: [
          {
            xtype: "textfield",
            inputId: "token",
            value: token,
            editable: false,
          },
        ],
        buttons: [
          {
            xtype: "button",
            iconCls: "fa fa-clipboard",
            handler: async function(b) {
              await navigator.clipboard.writeText(token);
            },
            text: gettext("Copy"),
          },
          {
            text: gettext("Ok"),
            handler: function() {
              this.up("window").close();
            },
          },
        ],
      }).show();
    },

    onDeploy: async function() {
      let me = this;
      let view = me.getView();
      let selection = view.getSelection();
      if (!selection || selection.length < 1) {
        return;
      }

      let token = selection[0].data.token;

      const hostname = window.location.hostname;
      const powershellCommand =
        `[System.Net.ServicePointManager]::ServerCertificateValidationCallback={$true}; ` +
        `[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; ` +
        `iex(New-Object Net.WebClient).DownloadString("https://${hostname}:8008/plus/agent/install/win?t=${token}")`;

      Ext.create("Ext.window.Window", {
        modal: true,
        width: 600,
        title: gettext("Deployment Scripts"),
        layout: "form",
        bodyPadding: "10 0",
        items: [
          {
            text: gettext("Windows (Powershell)"),
            xtype: "textfield",
            inputId: "ps1-command",
            value: powershellCommand,
            editable: false,
          },
        ],
        buttons: [
          {
            xtype: "button",
            iconCls: "fa fa-clipboard",
            handler: async function(b) {
              await navigator.clipboard.writeText(powershellCommand);
            },
            text: gettext("Copy"),
          },
          {
            text: gettext("Ok"),
            handler: function() {
              this.up("window").close();
            },
          },
        ],
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

    render_valid: function(value) {
      if (value.toString() == "false") {
        icon = "check good";
        text = "Valid";
      } else {
        icon = "times critical";
        text = "Invalid";
      }

      return `<i class="fa fa-${icon}"></i> ${text}`;
    },

    init: function(view) {
      Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
    },
  },

  listeners: {
    beforedestroy: "stopStore",
    deactivate: "stopStore",
    activate: "startStore",
    itemdblclick: "onCopy",
  },

  store: {
    type: "diff",
    rstore: {
      type: "update",
      storeid: "proxmox-agent-tokens",
      model: "pbs-model-tokens",
      proxy: {
        type: "pbsplus",
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
      disabled: true,
    },
    {
      text: gettext("Deploy with Token"),
      xtype: "proxmoxButton",
      handler: "onDeploy",
      disabled: true,
    },
    {
      text: gettext("Revoke Token"),
      xtype: "proxmoxStdRemoveButton",
      baseurl: pbsPlusBaseUrl + "/api2/extjs/config/d2d-token",
      getUrl: (rec) =>
        pbsPlusBaseUrl +
        `/api2/extjs/config/d2d-token/${encodeURIComponent(encodePathValue(rec.getId()))}`,
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
