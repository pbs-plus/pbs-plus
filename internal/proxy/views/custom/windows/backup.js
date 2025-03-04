Ext.define("PBS.D2DManagement.BackupWindow", {
  extend: "Proxmox.window.Edit",
  mixins: ["Proxmox.Mixin.CBind"],

  id: undefined,

  cbindData: function (config) {
    let me = this;
    return {
      warning: Ext.String.format(
        gettext("Manually start backup job '{0}'?"),
        me.id,
      ),
      id: me.id,
    };
  },

  title: gettext("Backup"),
  url: pbsPlusBaseUrl + `/api2/extjs/d2d/backup`,
  showProgress: true,
  submitUrl: function (url, values) {
    let id = values.id;
    delete values.id;
    return `${url}/${encodePathValue(id)}`;
  },
  submitOptions: {
    timeout: 120000,
  },

  layout: "hbox",
  width: 400,
  method: "POST",
  isCreate: true,
  submitText: gettext("Start Backup"),
  items: [
    {
      xtype: "container",
      padding: 0,
      layout: {
        type: "hbox",
        align: "stretch",
      },
      items: [
        {
          xtype: "component",
          cls: [
            Ext.baseCSSPrefix + "message-box-icon",
            Ext.baseCSSPrefix + "message-box-question",
            Ext.baseCSSPrefix + "dlg-icon",
          ],
        },
        {
          xtype: "container",
          flex: 1,
          items: [
            {
              xtype: "displayfield",
              cbind: {
                value: "{warning}",
              },
            },
            {
              xtype: "hidden",
              name: "id",
              cbind: {
                value: "{id}",
              },
            },
          ],
        },
      ],
    },
  ],
});

Ext.define("PBS.D2DManagement.StopBackupWindow", {
  extend: "Proxmox.window.Edit",
  mixins: ["Proxmox.Mixin.CBind"],

  id: undefined,
  upid: undefined,

  cbindData: function (config) {
    let me = this;
    return {
      warning: Ext.String.format(
        gettext("Stop backup job '{0}'?"),
        me.id,
      ),
      id: me.id,
    };
  },

  title: gettext("Stopping Backup Job"),
  url: '/api2/extjs/nodes',
  isCreate: true,
  showProgress: false,
  submitUrl: function (url, values) {
    let upid = values.upid;
    delete values.upid;

    let task = Proxmox.Utils.parse_task_upid(upid);

    return `${url}/${task.node}/tasks/${encodeURIComponent(upid)}`
  },
  submitOptions: {
    timeout: 120000,
  },

  layout: "hbox",
  width: 400,
  method: "DELETE",
  submitText: gettext("Stop Backup"),
  items: [
    {
      xtype: "container",
      padding: 0,
      layout: {
        type: "hbox",
        align: "stretch",
      },
      items: [
        {
          xtype: "component",
          cls: [
            Ext.baseCSSPrefix + "message-box-icon",
            Ext.baseCSSPrefix + "message-box-question",
            Ext.baseCSSPrefix + "dlg-icon",
          ],
        },
        {
          xtype: "container",
          flex: 1,
          items: [
            {
              xtype: "displayfield",
              cbind: {
                value: "{warning}",
              },
            },
            {
              xtype: "hidden",
              name: "id",
              cbind: {
                value: "{id}",
              },
            },
          ],
        },
      ],
    },
  ],
});
