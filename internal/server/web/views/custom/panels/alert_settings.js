/**
 * Alert Settings panel for D2D monitoring notifications.
 * Displayed as a tab in the D2D Management view.
 */
Ext.define("PBS.D2DManagement.Alerts", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsD2DAlertSettings",

  title: gettext("Alert Settings"),

  stateful: true,
  stateId: "grid-d2d-alert-settings-v1",

  controller: {
    xclass: "Ext.app.ViewController",

    editAlert: function () {
      var me = this;
      var view = me.getView();
      var selection = view.getSelection();
      if (!selection || selection.length !== 1) return;

      Ext.create("PBS.D2DManagement.AlertEditWindow", {
        record: selection[0],
        autoShow: true,
        listeners: {
          destroy: function () {
            me.reload();
          },
        },
      });
    },

    reload: function () {
      this.getView().getStore().load();
    },
  },

  listeners: {
    itemdblclick: "editAlert",
  },

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

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editAlert",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    "-",
    {
      xtype: "proxmoxButton",
      text: gettext("Reload"),
      handler: "reload",
      selModel: false,
    },
  ],

  columns: [
    {
      header: gettext("Alert Type"),
      dataIndex: "name",
      flex: 1,
      renderer: function (val) {
        var labels = {
          "stale-backup": "Stale Backup",
          "unconfigured-target": "Unconfigured Target",
          "target-offline": "Target Offline",
        };
        return labels[val] || val;
      },
      sortable: true,
    },
    {
      header: gettext("Enabled"),
      dataIndex: "enabled",
      width: 80,
      renderer: Proxmox.Utils.format_boolean,
      sortable: true,
    },
    {
      header: gettext("Threshold"),
      dataIndex: "threshold",
      width: 100,
      renderer: function (val, meta, record) {
        if (record.get("name") === "stale-backup") {
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
        var h = Math.floor(val / 60);
        var m = val % 60;
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
        var colors = {
          info: "blue",
          notice: "blue",
          warning: "orange",
          error: "red",
        };
        var color = colors[val] || "black";
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

  viewModel: {
    data: {
      isStaleBackup: false,
      cooldownHours: 24,
      cooldownMinutes: 0,
    },
  },

  initComponent: function () {
    var me = this;
    var rec = me.initialConfig.record;
    var name = rec ? rec.get("name") : "";

    me.url = "/api2/json/d2d/alert-settings/" + encodeURIComponent(name);
    me.method = "PUT";
    me.autoLoad = !!name;
    me.alertName = name;

    me.callParent(arguments);
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
            var labels = {
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

      onSetValues: function (values) {
        var panel = this;
        var vm = panel.up("pbsPlusWindowEdit").getViewModel();

        if (vm) {
          vm.set("isStaleBackup", values.name === "stale-backup");

          var totalMin = values["cooldown-minutes"] || 1440;
          vm.set("cooldownHours", Math.floor(totalMin / 60));
          vm.set("cooldownMinutes", totalMin % 60);
        }

        // Quiet-days checkboxes are set after render via a defer
        var quietDays = values["quiet-days"] || [];
        Ext.defer(function () {
          var quietGroup = panel.down("checkboxgroup[reference=quietDays]");
          if (quietGroup) {
            Ext.Array.each(quietGroup.items.items, function (cb) {
              cb.setValue(Ext.Array.contains(quietDays, cb.inputValue));
            });
          }
        }, 50);

        return values;
      },
    },
  ],

  onGetValues: function (values) {
    var me = this;
    var vm = me.getViewModel();

    values["cooldown-minutes"] =
      (vm.get("cooldownHours") || 0) * 60 +
      (vm.get("cooldownMinutes") || 0);

    // Collect checked quiet days
    var quietGroup = me.down("checkboxgroup[reference=quietDays]");
    if (quietGroup) {
      var checked = [];
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
