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
      { name: "skip-unscheduled", type: "bool" },
      "schedule-time",
      { name: "schedule-window-minutes", type: "int" },
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
      header: gettext("Skip Unscheduled"),
      dataIndex: "skip-unscheduled",
      width: 120,
      renderer: function (val, meta, record) {
        if (record.get("name") !== "stale-backup") return "-";
        return Proxmox.Utils.format_boolean(val);
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
      header: gettext("Schedule Time"),
      dataIndex: "schedule-time",
      width: 130,
      renderer: function (val, meta, record) {
        if (!val) return gettext("Any time");
        var window = record.get("schedule-window-minutes") || 60;
        var halfWindow = Math.floor(window / 2);
        return Ext.String.format("~{0} (±{1}m)", val, halfWindow);
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
 * Alert Setting Edit Window — includes exclusions management.
 */
Ext.define("PBS.D2DManagement.AlertEditWindow", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsD2DAlertEditWindow",

  title: gettext("Edit Alert Setting"),
  isCreate: false,
  width: 650,

  viewModel: {
    data: {
      isStaleBackup: false,
      isTargetAlert: false,
      cooldownHours: 24,
      cooldownMinutes: 0,
      hasSchedule: false,
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

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "inputpanel",
        reference: "alert-inputpanel",

        onSetValues: function (values) {
          var panel = this;
          var vm = panel.up("pbsPlusWindowEdit").getViewModel();

          if (vm) {
            vm.set("isStaleBackup", values.name === "stale-backup");
            vm.set(
              "isTargetAlert",
              values.name === "unconfigured-target" ||
                values.name === "target-offline"
            );

            var totalMin = values["cooldown-minutes"] || 1440;
            vm.set("cooldownHours", Math.floor(totalMin / 60));
            vm.set("cooldownMinutes", totalMin % 60);

            vm.set("hasSchedule", !!values["schedule-time"]);
          }

          // Check quiet-days checkboxes after render
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

        onGetValues: function (values) {
          var panel = this;
          var vm = panel.up("pbsPlusWindowEdit").getViewModel();

          // Build cooldown-minutes from hours + minutes
          if (vm) {
            values["cooldown-minutes"] =
              (vm.get("cooldownHours") || 0) * 60 +
              (vm.get("cooldownMinutes") || 0);

            // Clear schedule-time if checkbox is unchecked
            if (!vm.get("hasSchedule")) {
              values["schedule-time"] = "";
              values["schedule-window-minutes"] = "60";
            }
          }

          // Collect quiet-days from checkboxgroup, remove raw quiet-day entries
          var quietGroup = panel.down("checkboxgroup[reference=quietDays]");
          if (quietGroup) {
            var checked = [];
            Ext.Array.each(quietGroup.items.items, function (cb) {
              if (cb.checked) {
                checked.push(cb.inputValue);
              }
            });
            values["quiet-days"] = JSON.stringify(checked);
          }
          delete values["quiet-day"];

          // Remove internal hasSchedule tracker
          delete values["hasSchedule"];

          return values;
        },

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
          {
            xtype: "proxmoxcheckbox",
            name: "skip-unscheduled",
            fieldLabel: gettext("Skip Unscheduled"),
            boxLabel: gettext("Skip jobs without a schedule"),
            inputValue: 1,
            uncheckedValue: 0,
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
              { boxLabel: "Mon", inputValue: "Monday" },
              { boxLabel: "Tue", inputValue: "Tuesday" },
              {
                boxLabel: "Wed",
                inputValue: "Wednesday",
              },
              { boxLabel: "Thu", inputValue: "Thursday" },
              { boxLabel: "Fri", inputValue: "Friday" },
              { boxLabel: "Sat", inputValue: "Saturday" },
              { boxLabel: "Sun", inputValue: "Sunday" },
            ],
          },
          {
            xtype: "fieldcontainer",
            fieldLabel: gettext("Alert Time"),
            layout: "hbox",
            items: [
              {
                xtype: "proxmoxcheckbox",
                reference: "hasSchedule",
                boxLabel: gettext("Only alert around"),
                uncheckedValue: 0,
                inputValue: 1,
              },
              {
                xtype: "timefield",
                name: "schedule-time",
                reference: "scheduleTime",
                format: "H:i",
                submitFormat: "H:i",
                width: 100,
                margins: "0 0 0 8",
                allowBlank: true,
                bind: {
                  disabled: "{!hasSchedule}",
                },
              },
              {
                xtype: "displayfield",
                value: gettext("±"),
                width: 20,
                margins: "0 2 0 5",
                bind: {
                  hidden: "{!hasSchedule}",
                },
              },
              {
                xtype: "proxmoxintegerfield",
                name: "schedule-window-minutes",
                reference: "scheduleWindow",
                minValue: 10,
                maxValue: 720,
                value: 60,
                width: 60,
                allowBlank: false,
                bind: {
                  disabled: "{!hasSchedule}",
                  hidden: "{!hasSchedule}",
                },
              },
              {
                xtype: "displayfield",
                value: gettext("min window"),
                width: 75,
                margins: "0 0 0 2",
                bind: {
                  hidden: "{!hasSchedule}",
                },
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
      },
      {
        title: gettext("Exclusions"),
        xtype: "panel",
        layout: "fit",
        reference: "exclusions-panel",
        items: [
          {
            xtype: "grid",
            reference: "exclusionGrid",
            border: 0,

            store: {
              fields: [
                { name: "id", type: "int" },
                "alert-type",
                "exclude-type",
                "exclude-value",
                "comment",
              ],
              autoLoad: false,
              proxy: {
                type: "pbsplus",
                url: pbsPlusBaseUrl + "/api2/json/d2d/alert-exclusions",
              },
            },

            tbar: [
              {
                text: gettext("Add Job"),
                iconCls: "fa fa-plus",
                handler: "addJobExclusion",
                bind: {
                  disabled: "{isTargetAlert}",
                },
              },
              {
                text: gettext("Add Target"),
                iconCls: "fa fa-plus",
                handler: "addTargetExclusion",
                bind: {
                  disabled: "{isStaleBackup}",
                },
              },
              {
                text: gettext("Remove"),
                iconCls: "fa fa-trash-o",
                handler: "removeExclusion",
                disabled: true,
                enableFn: function () {
                  return (
                    this.up("grid").getSelection().length > 0
                  );
                },
              },
              "-",
              {
                text: gettext("Reload"),
                iconCls: "fa fa-refresh",
                handler: "reloadExclusions",
              },
            ],

            columns: [
              {
                header: gettext("Type"),
                dataIndex: "exclude-type",
                width: 90,
                renderer: function (val) {
                  return val === "job" ? "Job" : "Target";
                },
              },
              {
                header: gettext("Name"),
                dataIndex: "exclude-value",
                flex: 1,
              },
              {
                header: gettext("Comment"),
                dataIndex: "comment",
                flex: 1,
              },
            ],
          },
        ],
      },
    ],
  },

  controller: {
    xclass: "Ext.app.ViewController",

    init: function (view) {
      var me = this;

      // Load exclusions after the window has rendered and the alert name is known
      Ext.defer(function () {
        me.loadExclusions();
      }, 200);
    },

    loadExclusions: function () {
      var me = this;
      var view = me.getView();
      var grid = me.lookup("exclusionGrid");
      if (!grid || !view.alertName) return;

      var store = grid.getStore();
      if (!store) return;

      store.getProxy().setUrl(
        pbsPlusBaseUrl +
          "/api2/json/d2d/alert-exclusions?type=" +
          encodeURIComponent(view.alertName)
      );
      store.load();
    },

    reloadExclusions: function () {
      this.loadExclusions();
    },

    addJobExclusion: function () {
      var me = this;
      var view = me.getView();

      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/backup",
        method: "GET",
        success: function (resp) {
          var jobs = resp.result.data || [];
          var items = jobs.map(function (j) {
            return [j.id, "Backup: " + j.id + " (" + j.target + ")"];
          });

          me.showExclusionPicker("job", items);
        },
      });
    },

    addTargetExclusion: function () {
      var me = this;
      var view = me.getView();

      PBS.PlusUtils.API2Request({
        url: "/api2/json/d2d/target",
        method: "GET",
        success: function (resp) {
          var targets = resp.result.data || [];
          var items = targets.map(function (t) {
            return [t.name, t.name];
          });

          me.showExclusionPicker("target", items);
        },
      });
    },

    showExclusionPicker: function (excludeType, items) {
      var me = this;
      var view = me.getView();

      var win = Ext.create("Ext.window.Window", {
        title: Ext.String.format(
          "Add {0} Exclusion",
          excludeType === "job" ? "Job" : "Target"
        ),
        modal: true,
        width: 400,
        layout: "fit",
        items: [
          {
            xtype: "form",
            bodyPadding: 10,
            items: [
              {
                xtype: "proxmoxKVComboBox",
                name: "exclude-value",
                fieldLabel: excludeType === "job" ? "Job" : "Target",
                comboItems: items,
                allowBlank: false,
                editable: false,
              },
              {
                xtype: "proxmoxtextfield",
                name: "comment",
                fieldLabel: gettext("Comment"),
                allowBlank: true,
              },
            ],
          },
        ],
        buttons: [
          {
            text: gettext("Cancel"),
            handler: function () {
              win.close();
            },
          },
          {
            text: gettext("Add"),
            handler: function () {
              var form = win.down("form");
              var vals = form.getValues();

              if (!vals["exclude-value"]) {
                return;
              }

              PBS.PlusUtils.API2Request({
                url: "/api2/json/d2d/alert-exclusions",
                method: "POST",
                params: {
                  "alert-type": view.alertName,
                  "exclude-type": excludeType,
                  "exclude-value": vals["exclude-value"],
                  comment: vals.comment || "",
                },
                success: function () {
                  win.close();
                  me.loadExclusions();
                },
              });
            },
          },
        ],
      }).show();
    },

    removeExclusion: function () {
      var me = this;
      var grid = me.lookup("exclusionGrid");
      if (!grid) return;

      var selection = grid.getSelection();
      if (!selection || selection.length === 0) return;

      var exclusion = selection[0];
      PBS.PlusUtils.API2Request({
        url:
          "/api2/json/d2d/alert-exclusions/" + exclusion.get("id"),
        method: "DELETE",
        success: function () {
          me.loadExclusions();
        },
      });
    },
  },
});
