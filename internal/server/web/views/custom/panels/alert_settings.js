/**
 * Alert Settings panel for D2D monitoring notifications.
 * Displayed as a tab in the D2D Management view.
 */
Ext.define("PBS.D2DManagement.Alerts", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsD2DAlertSettings",

  title: gettext("Alert Settings"),
  reference: "d2d-alert-settings",

  controller: {
    type: "controller",

    onEdit: function () {
      let me = this;
      let view = me.getView();
      let sel = view.getSelection();
      if (!sel.length) return;

      Ext.create("PBS.D2DManagement.AlertEditWindow", {
        record: sel[0],
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
      header: gettext("Cooldown"),
      dataIndex: "cooldown-minutes",
      width: 110,
      renderer: function (val) {
        if (!val) return "-";
        if (val < 60) return Ext.String.format("{0} min", val);
        let h = Math.floor(val / 60);
        let m = val % 60;
        if (m === 0) return Ext.String.format("{0}h", h);
        return Ext.String.format("{0}h {1}m", h, m);
      },
    },
    {
      header: gettext("Quiet Days"),
      dataIndex: "quiet-days",
      width: 180,
      renderer: function (val) {
        if (!val || !val.length) return "-";
        return val.join(", ");
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
          '<span style="color:' +
          color +
          ';font-weight:bold">' +
          val +
          "</span>"
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
      { name: "cooldown-minutes", type: "int" },
      "quiet-days",
    ],
    autoLoad: true,
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/alert-settings",
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
  width: 550,

  url: "/api2/json/d2d/alert-settings",

  viewModel: {
    data: {
      isStaleBackup: false,
      cooldownHours: 24,
      cooldownMinutes: 0,
    },
  },

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
          xtype: "fieldcontainer",
          fieldLabel: gettext("Cooldown"),
          layout: "hbox",
          items: [
            {
              xtype: "proxmoxintegerfield",
              reference: "cooldownHours",
              minValue: 0,
              maxValue: 720,
              width: 70,
              bind: {
                value: "{cooldownHours}",
              },
              listeners: {
                change: function (field, val) {
                  let win = field.up("pbsPlusWindowEdit");
                  let vm = win.getViewModel();
                  vm.set(
                    "cooldownHours",
                    val || 0
                  );
                },
              },
            },
            {
              xtype: "displayfield",
              value: "h",
              width: 25,
              margins: "0 5 0 2",
            },
            {
              xtype: "proxmoxintegerfield",
              reference: "cooldownMinutes",
              minValue: 0,
              maxValue: 59,
              width: 60,
              bind: {
                value: "{cooldownMinutes}",
              },
              listeners: {
                change: function (field, val) {
                  let win = field.up("pbsPlusWindowEdit");
                  let vm = win.getViewModel();
                  vm.set(
                    "cooldownMinutes",
                    val || 0
                  );
                },
              },
            },
            {
              xtype: "displayfield",
              value: "m",
              width: 25,
              margins: "0 0 0 2",
            },
          ],
        },
        {
          xtype: "checkboxgroup",
          reference: "quietDays",
          fieldLabel: gettext("Quiet Days"),
          columns: 4,
          items: [
            {
              boxLabel: "Mon",
              name: "quiet-day",
              inputValue: "Monday",
            },
            {
              boxLabel: "Tue",
              name: "quiet-day",
              inputValue: "Tuesday",
            },
            {
              boxLabel: "Wed",
              name: "quiet-day",
              inputValue: "Wednesday",
            },
            {
              boxLabel: "Thu",
              name: "quiet-day",
              inputValue: "Thursday",
            },
            {
              boxLabel: "Fri",
              name: "quiet-day",
              inputValue: "Friday",
            },
            {
              boxLabel: "Sat",
              name: "quiet-day",
              inputValue: "Saturday",
            },
            {
              boxLabel: "Sun",
              name: "quiet-day",
              inputValue: "Sunday",
            },
          ],
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
        let vm = panel.up("pbsPlusWindowEdit").getViewModel();

        if (vm) {
          vm.set("isStaleBackup", values.name === "stale-backup");

          // Split cooldown-minutes into hours + minutes
          let totalMin = values["cooldown-minutes"] || 1440;
          vm.set("cooldownHours", Math.floor(totalMin / 60));
          vm.set("cooldownMinutes", totalMin % 60);
        }

        // Check quiet-days checkboxes
        let quietDays = values["quiet-days"] || [];
        let quietGroup = panel.down("checkboxgroup[reference=quietDays]");
        if (quietGroup) {
          Ext.Array.each(quietGroup.items.items, function (cb) {
            cb.setValue(
              Ext.Array.contains(quietDays, cb.inputValue)
            );
          });
        }

        panel.callParent(arguments);
      },
    },
  ],

  initComponent: function () {
    let me = this;

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
    let me = this;
    let values = me.callParent(arguments);

    if (me.record) {
      values.name = me.record.get("name");
    }

    // Compute cooldown-minutes from hours + minutes
    let vm = me.getViewModel();
    values["cooldown-minutes"] =
      (vm.get("cooldownHours") || 0) * 60 +
      (vm.get("cooldownMinutes") || 0);

    // Collect checked quiet days
    let quietGroup = me.down("checkboxgroup[reference=quietDays]");
    if (quietGroup) {
      let checked = [];
      Ext.Array.each(quietGroup.items.items, function (cb) {
        if (cb.checked) {
          checked.push(cb.inputValue);
        }
      });
      values["quiet-days"] = JSON.stringify(checked);
    }

    return values;
  },
});
