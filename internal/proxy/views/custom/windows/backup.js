Ext.define('PBS.D2DManagement.BackupWindow', {
  extend: 'Proxmox.window.Edit',
  mixins: ['Proxmox.Mixin.CBind'],

  // NEW: array of job‐IDs to start
  ids: undefined,

  cbindData: function (config) {
    let me  = this;
    let ids = me.ids || (me.id ? [me.id] : []);
    let list = ids.map(Ext.String.htmlEncode).join("', '");
    let warning =
      ids.length > 1
        ? Ext.String.format(
            gettext("Manually start backup jobs '{0}'?"),
            list,
          )
        : Ext.String.format(
            gettext("Manually start backup job '{0}'?"),
            list,
          );
    return { warning: warning };
  },

  title: gettext('Backup'),
  url: pbsPlusBaseUrl + '/api2/extjs/d2d/backup',
  showProgress: false,
  method: 'POST',
  submitText: gettext('Start Backup'),
  submitOptions: { timeout: 120000 },

  submit: function () {
    let me  = this;
    let ids = me.ids || [];

    // build an array of Promises
    let promises = ids.map((id) => {
      let url = me.submitUrl(me.url, { id: id });
      return new Promise((resolve, reject) => {
        Proxmox.Utils.API2Request({
          url: url,
          method: 'POST',
          waitMsgTarget: me,
          success: () => resolve(),
          failure: (resp) => reject(resp),
        });
      });
    });

    Promise.all(promises)
      .then(() => me.close())
      .catch((resp) => {
        Ext.Msg.alert(gettext('Error'), resp.htmlStatus);
      });
  },

  layout: 'hbox',
  width: 400,
  items: [
    {
      xtype: 'container',
      padding: 0,
      layout: { type: 'hbox', align: 'stretch' },
      items: [
        {
          xtype: 'component',
          cls: [
            Ext.baseCSSPrefix + 'message-box-icon',
            Ext.baseCSSPrefix + 'message-box-question',
            Ext.baseCSSPrefix + 'dlg-icon',
          ],
        },
        {
          xtype: 'container',
          flex: 1,
          items: [
            {
              xtype: 'displayfield',
              cbind: { value: '{warning}' },
            },
          ],
        },
      ],
    },
  ],
});

Ext.define('PBS.D2DManagement.StopBackupWindow', {
  extend: 'Proxmox.window.Edit',
  mixins: ['Proxmox.Mixin.CBind'],

  // NEW: array of jobs to stop
  jobs: undefined,

  cbindData: function (config) {
    let me   = this;
    let jobs = me.jobs || [];
    let ids  = jobs.map((j) => Ext.String.htmlEncode(j.id)).join("', '");
    let warning =
      jobs.length > 1
        ? Ext.String.format(
            gettext("Stop backup jobs '{0}'?"),
            ids,
          )
        : Ext.String.format(
            gettext("Stop backup job '{0}'?"),
            ids,
          );
    return { warning: warning };
  },

  title: gettext('Stopping Backup Job(s)'),
  url: '/api2/extjs/nodes',
  showProgress: false,
  method: 'DELETE',
  submitText: gettext('Stop Backup'),
  submitOptions: { timeout: 120000 },

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

  submit: function () {
    let me   = this;
    let jobs = me.jobs || [];

    let promises = jobs.map((job) => {
      // first delete the “Plus” queued item if present
      const plusCall = () =>
        !job.hasPlusJob
          ? Promise.resolve()
          : new Promise((resolve) => {
              Proxmox.Utils.API2Request({
                url: me.submitPlusUrl(job),
                method: 'DELETE',
                waitMsgTarget: me,
                // on both success _and_ failure we proceed
                success: () => resolve(),
                failure: () => resolve(),
              });
            });

      // then delete the PBS-side task if present
      const taskCall = () =>
        !job.hasPBSTask
          ? Promise.resolve()
          : new Promise((resolve, reject) => {
              Proxmox.Utils.API2Request({
                url: me.submitUrl(me.url, job),
                method: 'DELETE',
                waitMsgTarget: me,
                success: () => resolve(),
                failure: (resp) => reject(resp),
              });
            });

      return plusCall().then(taskCall);
    });

    Promise.all(promises)
      .then(() => me.close())
      .catch((resp) => {
        Ext.Msg.alert(gettext('Error'), resp.htmlStatus);
      });
  },

  layout: 'hbox',
  width: 400,
  items: [
    {
      xtype: 'container',
      padding: 0,
      layout: { type: 'hbox', align: 'stretch' },
      items: [
        {
          xtype: 'component',
          cls: [
            Ext.baseCSSPrefix + 'message-box-icon',
            Ext.baseCSSPrefix + 'message-box-question',
            Ext.baseCSSPrefix + 'dlg-icon',
          ],
        },
        {
          xtype: 'container',
          flex: 1,
          items: [
            {
              xtype: 'displayfield',
              cbind: { value: '{warning}' },
            },
          ],
        },
      ],
    },
  ],
});
