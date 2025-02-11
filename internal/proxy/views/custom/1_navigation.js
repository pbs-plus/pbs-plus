Ext.define("PBS.store.NavigationStore", {
  extend: "Ext.data.TreeStore",

  storeId: "NavigationStore",

  root: {
    expanded: true,
    children: [
      {
        text: gettext("Dashboard"),
        iconCls: "fa fa-tachometer",
        path: "pbsDashboard",
        leaf: true,
      },
      {
        text: gettext("Notes"),
        iconCls: "fa fa-sticky-note-o",
        path: "pbsNodeNotes",
        leaf: true,
      },
      {
        text: gettext("Configuration"),
        iconCls: "fa fa-gears",
        path: "pbsSystemConfiguration",
        expanded: true,
        children: [
          {
            text: gettext("Access Control"),
            iconCls: "fa fa-key",
            path: "pbsAccessControlPanel",
            leaf: true,
          },
          {
            text: gettext("Remotes"),
            iconCls: "fa fa-server",
            path: "pbsRemoteView",
            leaf: true,
          },
          {
            text: gettext("Traffic Control"),
            iconCls: "fa fa-signal fa-rotate-90",
            path: "pbsTrafficControlView",
            leaf: true,
          },
          {
            text: gettext("Certificates"),
            iconCls: "fa fa-certificate",
            path: "pbsCertificateConfiguration",
            leaf: true,
          },
          {
            text: gettext("Notifications"),
            iconCls: "fa fa-bell-o",
            path: "pbsNotificationConfigView",
            leaf: true,
          },
          {
            text: gettext("Subscription"),
            iconCls: "fa fa-support",
            path: "pbsSubscription",
            leaf: true,
          },
        ],
      },
      {
        text: gettext("Administration"),
        iconCls: "fa fa-wrench",
        path: "pbsServerAdministration",
        expanded: true,
        leaf: false,
        children: [
          {
            text: gettext("Shell"),
            iconCls: "fa fa-terminal",
            path: "pbsXtermJsConsole",
            leaf: true,
          },
          {
            text: gettext("Storage / Disks"),
            iconCls: "fa fa-hdd-o",
            path: "pbsStorageAndDiskPanel",
            leaf: true,
          },
        ],
      },
      {
        text: "Disk Backup",
        iconCls: "fa fa-hdd-o",
        id: "backup_targets",
        path: "pbsD2DManagement",
        expanded: true,
        children: [],
      },
      {
        text: "Tape Backup",
        iconCls: "pbs-icon-tape",
        id: "tape_management",
        path: "pbsTapeManagement",
        expanded: true,
        children: [],
      },
      {
        text: gettext("Datastore"),
        iconCls: "fa fa-archive",
        id: "datastores",
        path: "pbsDataStores",
        expanded: true,
        expandable: false,
        leaf: false,
        children: [
          {
            text: gettext("Add Datastore"),
            iconCls: "fa fa-plus-circle",
            leaf: true,
            id: "addbutton",
            virtualEntry: true,
          },
        ],
      },
    ],
  },
});
