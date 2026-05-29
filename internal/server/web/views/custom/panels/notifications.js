// Shared Notifications tab panel for PBS Plus D2D job edit windows.
// Matches the Proxmox Backup Server tape backup job notification tab pattern.
// Provides two modes: notification-system (default) and legacy-sendmail.

Ext.define("PBS.D2D.NotificationInputPanel", {
  extend: "Proxmox.panel.InputPanel",
  xtype: "pbsD2DNotificationPanel",

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

  onGetValues: function (values) {
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
      xtype: "component",
      html:
        '<span class="pmx-hint" style="display:block;padding:8px 6px;font-size:11px;line-height:16px;">' +
        gettext("The notification mode controls how job completion and failure notifications are delivered.") +
        " " +
        gettext("'Use global notification settings' routes notifications through the PBS notification system, which supports email, Gotify, SMTP, and webhook endpoints configured in Datacenter &rarr; Notifications.") +
        " " +
        gettext("'Use sendmail to send an email (legacy)' sends notifications directly via the system's sendmail command.") +
        "</span>",
      margin: "10 0 0 50",
    },
  ],
});

// Factory function to create a Notifications tab config for use in tabpanel items arrays.
// Usage: items: [ optionsTab, PBS.D2DManagement.makeNotificationTab() ]
PBS.D2DManagement.makeNotificationTab = function () {
  return {
    xtype: "pbsD2DNotificationPanel",
    title: gettext("Notifications"),
  };
};
