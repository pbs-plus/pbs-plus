// Shared notification tab factories for PBS Plus D2D job/batch edit windows.

// Full notification tab for job edit windows (backup, restore, verification).
// Includes notification mode selection and batch assignment combo.
// Usage: items: [ optionsTab, PBS.D2DManagement.makeNotificationTab() ]
PBS.D2DManagement.makeNotificationTab = function () {
  return {
    xtype: "inputpanel",
    title: gettext("Notifications"),

    viewModel: {
      data: {
        notificationMode: undefined,
      },
      formulas: {
        notificationSystemSelected: function (get) {
          var mode = get("notificationMode");
          if (!mode || !mode["notification-mode"]) {
            return true;
          }
          return mode["notification-mode"] === "notification-system";
        },
      },
    },

    onSetValues: function (values) {
      var me = this;
      var mode = values["notification-mode"];
      me.getViewModel().set("notificationMode", {
        "notification-mode": mode || "notification-system",
      });
      return values;
    },

    items: [
      {
        xtype: "radiogroup",
        height: "15px",
        layout: {
          type: "vbox",
        },
        bind: {
          value: "{notificationMode}",
        },
        items: [
          {
            xtype: "radiofield",
            name: "notification-mode",
            inputValue: "notification-system",
            boxLabel: gettext("Use global notification settings"),
            checked: true,
          },
          {
            xtype: "radiofield",
            name: "notification-mode",
            inputValue: "legacy-sendmail",
            boxLabel: gettext("Use sendmail to send an email (legacy)"),
          },
        ],
      },
      {
        xtype: "pmxUserSelector",
        name: "notify-user",
        fieldLabel: gettext("Recipient"),
        emptyText: "root@pam",
        allowBlank: true,
        value: null,
        renderer: Ext.String.htmlEncode,
        padding: "0 0 0 50",
        disabled: true,
        bind: {
          disabled: "{notificationSystemSelected}",
        },
      },
      {
        xtype: "box",
        autoEl: { tag: "hr" },
        margin: "10 0",
      },
      {
        xtype: "displayfield",
        value: gettext(
          "Assign this job to a notification batch to consolidate notifications " +
            "across multiple jobs. When all jobs in a batch complete (or the timeout " +
            "fires), a single consolidated notification is sent instead of one per job."
        ),
        fieldLabel: gettext("Notification Batch"),
        labelAlign: "top",
      },
      {
        xtype: "combo",
        name: "notification-batch",
        fieldLabel: gettext("Batch"),
        emptyText: gettext("No batch (individual notification)"),
        queryMode: "remote",
        store: {
          fields: ["name", "comment"],
          autoLoad: true,
          proxy: {
            type: "pbsplus",
            url: pbsPlusBaseUrl + "/api2/json/d2d/notification-batch",
          },
        },
        displayField: "name",
        valueField: "name",
        allowBlank: true,
        editable: false,
        forceSelection: true,
        autoSelect: false,
        renderer: function (v) {
          return v ? Ext.String.htmlEncode(v) : "-";
        },
      },
    ],
  };
};

// Simplified notification tab for batch edit windows.
// Only includes notification mode selection (no batch assignment).
// Usage: items: [ optionsTab, PBS.D2DManagement.makeSimpleNotificationTab() ]
PBS.D2DManagement.makeSimpleNotificationTab = function () {
  return {
    xtype: "inputpanel",
    title: gettext("Notifications"),

    viewModel: {
      data: {
        notificationMode: undefined,
      },
      formulas: {
        notificationSystemSelected: function (get) {
          var mode = get("notificationMode");
          if (!mode || !mode["notification-mode"]) {
            return true;
          }
          return mode["notification-mode"] === "notification-system";
        },
      },
    },

    onSetValues: function (values) {
      var me = this;
      var mode = values["notification-mode"];
      me.getViewModel().set("notificationMode", {
        "notification-mode": mode || "notification-system",
      });
      return values;
    },

    items: [
      {
        xtype: "radiogroup",
        height: "15px",
        layout: {
          type: "vbox",
        },
        bind: {
          value: "{notificationMode}",
        },
        items: [
          {
            xtype: "radiofield",
            name: "notification-mode",
            inputValue: "notification-system",
            boxLabel: gettext("Use global notification settings"),
            checked: true,
          },
          {
            xtype: "radiofield",
            name: "notification-mode",
            inputValue: "legacy-sendmail",
            boxLabel: gettext("Use sendmail to send an email (legacy)"),
          },
        ],
      },
      {
        xtype: "pmxUserSelector",
        name: "notify-user",
        fieldLabel: gettext("Recipient"),
        emptyText: "root@pam",
        allowBlank: true,
        value: null,
        renderer: Ext.String.htmlEncode,
        padding: "0 0 0 50",
        disabled: true,
        bind: {
          disabled: "{notificationSystemSelected}",
        },
      },
    ],
  };
};
