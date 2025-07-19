Ext.define("PBS.D2DManagement.BackupWindow", {
  extend: "Proxmox.window.Edit",
  mixins: ["Proxmox.Mixin.CBind"],

  ids: undefined, // array of job IDs

  cbindData(config) {
    let me  = this;
    let ids = me.ids || (me.id ? [me.id] : []);
    let list = ids.map(Ext.String.htmlEncode).join("', '");
    let warning =
      ids.length > 1
        ? Ext.String.format(
            gettext("Manually start backup jobs '{0}'?"),
            list
          )
        : Ext.String.format(
            gettext("Manually start backup job '{0}'?"),
            list
          );
    return { warning };
  },

  title: gettext("Backup"),
  url: pbsPlusBaseUrl + "/api2/extjs/d2d/backup",
  submitText: undefined,
  method:     undefined,
  showProgress: false,
  submitOptions: { timeout: 120000 },

  // clear the default buttons
  buttons: [],

  // our own bottom bar
  bbar: [
    "->",
    {
      text: gettext("Cancel"),
      handler: function (btn) {
        btn.up("window").close();
      },
    },
    {
      text: gettext("Start Backup"),
      // always enabled
      formBind: false,
      disabled: false,
      handler: function (btn) {
        btn.up("window").submit();
      },
    },
  ],

  layout: "hbox",
  width: 400,
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
      xtype: "component",
      flex: 1,
      cbind: { html: "{warning}" },
      style: "white-space: normal; word-wrap: break-word;",
    },
  ],

  submit() {
    let me  = this;
    let ids = me.ids || [];
    let calls = ids.map((id) => {
      let url = me.submitUrl(me.url, { id });
      return new Promise((res, rej) => {
        Proxmox.Utils.API2Request({
          url: url,
          method: "POST",
          waitMsgTarget: me,
          success: () => res(),
          failure: (resp) => rej(resp),
        });
      });
    });

    Promise.all(calls)
      .then(() => me.close())
      .catch((resp) => {
        Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
      });
  },
});

Ext.define("PBS.D2DManagement.StopBackupWindow", {
  extend: "Proxmox.window.Edit",
  mixins: ["Proxmox.Mixin.CBind"],

  jobs: undefined, // array of {id, upid, hasPlusJob, hasPBSTask}

  cbindData(config) {
    let me   = this;
    let jobs = me.jobs || [];
    let ids  = jobs.map((j) => Ext.String.htmlEncode(j.id)).join("', '");
    let warning =
      jobs.length > 1
        ? Ext.String.format(
            gettext("Stop backup jobs '{0}'?"),
            ids
          )
        : Ext.String.format(
            gettext("Stop backup job '{0}'?"),
            ids
          );
    return { warning };
  },

  title: gettext("Stopping Backup Job(s)"),
  url: "/api2/extjs/nodes",
  submitText: undefined,
  method:     undefined,
  showProgress: false,
  submitOptions: { timeout: 120000 },

  // clear default buttons
  buttons: [],

  bbar: [
    "->",
    {
      text: gettext("Cancel"),
      handler: function (btn) {
        btn.up("window").close();
      },
    },
    {
      text: gettext("Stop Backup"),
      formBind: false,
      disabled: false,
      handler: function (btn) {
        btn.up("window").submit();
      },
    },
  ],

  layout: "hbox",
  width: 400,
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
      xtype: "component",
      flex: 1,
      cbind: { html: "{warning}" },
      style: "white-space: normal; word-wrap: break-word;",
    },
  ],

  // per‐job URL builders
  submitPlusUrl: function (job) {
    return (
      pbsPlusBaseUrl +
      '/api2/extjs/d2d/backup/' +
      encodeURIComponent(job.id)
    );
  },
  submitUrl: function (base, job) {
    let task = Proxmox.Utils.parse_task_upid(job.upid);
    return (
      base +
      '/' +
      task.node +
      '/tasks/' +
      encodeURIComponent(job.upid)
    );
  },

  submit() {
    let me   = this;
    let jobs = me.jobs || [];
    let calls = jobs.map((job) => {
      // first delete the “plus” queue entry
      let plusCall = () =>
        !job.hasPlusJob
          ? Promise.resolve()
          : new Promise((res) => {
              Proxmox.Utils.API2Request({
                url: me.submitPlusUrl(job),
                method: "DELETE",
                waitMsgTarget: me,
                success: () => res(),
                failure: () => res(),
              });
            });

      // then delete the PBS task
      let taskCall = () =>
        !job.hasPBSTask
          ? Promise.resolve()
          : new Promise((res, rej) => {
              Proxmox.Utils.API2Request({
                url: me.submitUrl(me.url, job),
                method: "DELETE",
                waitMsgTarget: me,
                success: () => res(),
                failure: (resp) => rej(resp),
              });
            });

      return plusCall().then(taskCall);
    });

    Promise.all(calls)
      .then(() => me.close())
      .catch((resp) => {
        Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
      });
  },
});
