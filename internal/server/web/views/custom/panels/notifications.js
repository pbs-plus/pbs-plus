// Shared Notifications tab config factory for PBS Plus D2D job edit windows.
// Matches the Proxmox Backup Server tape backup job notification tab pattern exactly.
// Provides two modes: notification-system (default) and legacy-sendmail.
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
    ],
  };
};
