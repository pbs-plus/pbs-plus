/**
 * Alert Settings panel for D2D monitoring notifications.
 * Displayed as a tab in the D2D Management view.
 */
Ext.define("PBS.D2DManagement.Alerts", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsD2DAlertSettings",

  title: gettext("Alert Settings"),
  reference: "d2d-alert-settings",

  controllers: {
    d2dAlertSettings: {
      type: "controller",

      onEdit: function () {
        let me = this;
        let view = me.getView();
        let sel = view.getSelection();
        if (!sel.length) return;

        let rec = sel[0];
        Ext.create("PBS.D2DManagement.AlertEditWindow", {
          record: rec,
          listeners: {
            destroy: function () {
              view.getStore().load();
            },
          },
        }).show();
      },

      onReload: function () {
        this.getView().getStore().load();
      },
    },
  },

  tbar: [
    {
      text: gettext("Edit"),
      xtype: "button",
      iconCls: "fa fa-pencil",
      handler: "onEdit",
      disabled: true,
      bind: {
        disabled: "{!alertGrid.selection}",
      },
    },
    "-",
    {
      text: gettext("Reload"),
      xtype: "button",
      iconCls: "fa fa-refresh",
      handler: "onReload",
    },
  ],

  columns: [
    {
      header: gettext("Alert Type"),
      dataIndex: "name",
      flex: 1,
      renderer: function (val) {
        let labels = {
          "stale-backup": "Stale Backup",
          "unconfigured-target": "Unconfigured Target",
          "target-offline": "Target Offline",
        };
        return labels[val] || val;
      },
    },
    {
      header: gettext("Enabled"),
      dataIndex: "enabled",
      width: 80,
      renderer: Proxmox.Utils.format_boolean,
    },
    {
      header: gettext("Threshold"),
      dataIndex: "threshold",
      width: 100,
      renderer: function (val, meta, record) {
        let name = record.get("name");
        if (name === "stale-backup") {
          return val ? Ext.String.format("{0} days", val) : "-";
        }
        return "-";
      },
    },
    {
      header: gettext("Severity"),
      dataIndex: "severity",
      width: 90,
      renderer: function (val) {
        let colors = {
          info: "blue",
          notice: "blue",
          warning: "orange",
          error: "red",
        };
        let color = colors[val] || "black";
        return (
          '<span style="color:' + color + ';font-weight:bold">' + val + "</span>"
        );
      },
    },
    {
      header: gettext("Comment"),
      dataIndex: "comment",
      flex: 1,
    },
    {
      header: gettext("Last Sent"),
      dataIndex: "last-sent",
      width: 160,
      renderer: function (val) {
        if (!val) return "-";
        return new Date(val * 1000).toLocaleString();
      },
    },
  ],

  store: {
    fields: [
      "name",
      "enabled",
      "threshold",
      "severity",
      "comment",
      { name: "last-sent", type: "int" },
    ],
    autoLoad: true,
    proxy: {
      type: "pbsplus",
      url: "/api2/json/d2d/alert-settings",
    },
    sorters: [
      {
        property: "name",
        direction: "ASC",
      },
    ],
  },

  bind: {
    selection: "{alertGrid.selection}",
  },
});

/**
 * Alert Setting Edit Window
 */
Ext.define("PBS.D2DManagement.AlertEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsD2DAlertEditWindow",

  title: gettext("Edit Alert Setting"),
  isCreate: false,
  width: 500,

  url: "/api2/json/d2d/alert-settings",

  items: [
    {
      xtype: "inputpanel",
      reference: "alert-inputpanel",

      column1: [
        {
          xtype: "displayfield",
          name: "name",
          fieldLabel: gettext("Alert Type"),
          renderer: function (val) {
            let labels = {
              "stale-backup": "Stale Backup",
              "unconfigured-target": "Unconfigured Target",
              "target-offline": "Target Offline",
            };
            return labels[val] || val;
          },
          submitValue: true,
        },
        {
          xtype: "proxmoxcheckbox",
          name: "enabled",
          fieldLabel: gettext("Enabled"),
          inputValue: 1,
          uncheckedValue: 0,
          checked: true,
        },
      ],

      column2: [
        {
          xtype: "proxmoxKVComboBox",
          name: "severity",
          fieldLabel: gettext("Severity"),
          value: "warning",
          comboItems: [
            ["info", "Info"],
            ["notice", "Notice"],
            ["warning", "Warning"],
            ["error", "Error"],
          ],
        },
        {
          xtype: "proxmoxintegerfield",
          name: "threshold",
          fieldLabel: gettext("Threshold (days)"),
          minValue: 1,
          maxValue: 365,
          allowBlank: true,
          bind: {
            disabled: "{!isStaleBackup}",
            visible: "{isStaleBackup}",
          },
        },
      ],

      columnB: [
        {
          xtype: "proxmoxtextfield",
          name: "comment",
          fieldLabel: gettext("Comment"),
          allowBlank: true,
        },
      ],

      setValues: function (values) {
        let panel = this;
        panel.callParent(arguments);

        // Show threshold field only for stale-backup alerts
        let vm = panel.up("pbsPlusWindowEdit").getViewModel();
        if (vm) {
          vm.set("isStaleBackup", values.name === "stale-backup");
        }
      },
    },
  ],

  initComponent: function () {
    let me = this;

    me.viewModel = {
      data: {
        isStaleBackup: false,
      },
    };

    if (me.record) {
      me.loadValues = me.record.data;
    }

    me.callParent(arguments);

    if (me.record) {
      me.load({
        params: { name: me.record.get("name") },
      });
    }
  },

  getValues: function () {
    let values = this.callParent(arguments);
    if (this.record) {
      values.name = this.record.get("name");
    }
    return values;
  },
});
