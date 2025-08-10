Ext.define('PBS.plusWindow.TaskViewer', {
  extend: 'Ext.window.Window',
  alias: 'widget.proxmoxPlusTaskViewer',

  extraTitle: '', // string to prepend after the generic task title
  taskDone: Ext.emptyFn,

  initComponent: function() {
    let me = this;

    if (!me.upid) {
      throw "No task specified";
    }

    let task = Proxmox.Utils.parse_task_upid(me.upid);
    // Declare statgrid first so it is available in renderer closures.
    let statgrid;

    let rows = {
      status: {
        header: gettext('Status'),
        defaultValue: 'unknown',
        renderer: function(value) {
          if (value !== 'stopped') {
            return Ext.htmlEncode(value);
          }
          // Use the statgrid instance to check for exitstatus.
          let es = statgrid.getObjectValue('exitstatus');
          if (es) {
            return Ext.htmlEncode(`${value}: ${es}`);
          }
          return 'unknown';
        }
      },
      exitstatus: {
        visible: false
      },
      type: {
        header: gettext('Task type'),
        required: true
      },
      user: {
        header: gettext('User name'),
        renderer: function(value) {
          let user = value;
          let tokenid = statgrid.getObjectValue('tokenid');
          if (tokenid) {
            user += `!${tokenid} (API Token)`;
          }
          return Ext.String.htmlEncode(user);
        },
        required: true
      },
      tokenid: {
        header: gettext('API Token'),
        renderer: Ext.String.htmlEncode,
        visible: false
      },
      node: {
        header: gettext('Node'),
        required: true
      },
      pid: {
        header: gettext('Process ID'),
        required: true
      },
      task_id: {
        header: gettext('Task ID')
      },
      starttime: {
        header: gettext('Start Time'),
        required: true,
        renderer: Proxmox.Utils.render_timestamp
      },
      upid: {
        header: gettext('Unique task ID'),
        renderer: Ext.String.htmlEncode
      }
    };

    // If an end time was passed, add the corresponding columns.
    if (me.endtime) {
      if (typeof me.endtime === 'object') {
        // Convert date to epoch seconds.
        me.endtime = parseInt(me.endtime.getTime() / 1000, 10);
      }
      rows.endtime = {
        header: gettext('End Time'),
        required: true,
        renderer: function() {
          return Proxmox.Utils.render_timestamp(me.endtime);
        }
      };
    }

    rows.duration = {
      header: gettext('Duration'),
      required: true,
      renderer: function() {
        let starttime = statgrid.getObjectValue('starttime');
        let endtime = me.endtime || Date.now() / 1000;
        let duration = endtime - starttime;
        return Proxmox.Utils.format_duration_human(duration);
      }
    };

    let statstore = Ext.create('Proxmox.data.ObjectStore', {
      url: `/api2/json/nodes/${task.node}/tasks/${encodeURIComponent(me.upid)}/status`,
      interval: 1000,
      rows: rows
    });

    // Ensure we stop the update when the window is destroyed.
    me.on('destroy', statstore.stopUpdate, statstore);

    const stopTask = () => {
      PBS.PlusUtils.API2Request({
        url: `/nodes/${task.node}/tasks/${encodeURIComponent(me.upid)}`,
        waitMsgTarget: me,
        method: 'DELETE',
        failure: response => Ext.Msg.alert(gettext('Error'), response.htmlStatus)
      });
    };

    let stopBtn1 = Ext.create('Ext.Button', {
      text: gettext('Stop'),
      disabled: true,
      handler: stopTask
    });

    let stopBtn2 = Ext.create('Ext.Button', {
      text: gettext('Stop'),
      disabled: true,
      handler: stopTask
    });

    statgrid = Ext.create('Proxmox.grid.ObjectGrid', {
      title: gettext('Status'),
      layout: 'fit',
      tbar: [stopBtn1],
      rstore: statstore,
      rows: rows,
      border: false
    });

    let downloadBtn = Ext.create('Ext.Button', {
      text: gettext('Download'),
      iconCls: 'fa fa-download',
      handler: () =>
        Proxmox.Utils.downloadAsFile(
          `/api2/json/nodes/${task.node}/tasks/${encodeURIComponent(
            me.upid
          )}/log?download=1`
        )
    });

    let logView = Ext.create('PBS.plusPanel.LogView', {
      title: gettext('Output'),
      tbar: [stopBtn2, '->', downloadBtn],
      border: false,
      url: `/api2/extjs/nodes/${task.node}/tasks/${encodeURIComponent(
        me.upid
      )}/log`
    });

    me.mon(statstore, 'load', () => {
      let status = statgrid.getObjectValue('status');

      if (status === 'stopped') {
        // Once the task stops, stop live updates.
        logView.scrollToEnd = false;
        logView.requestUpdate();
        statstore.stopUpdate();
        me.taskDone(statgrid.getObjectValue('exitstatus') === 'OK');
      }

      stopBtn1.setDisabled(status !== 'running');
      stopBtn2.setDisabled(status !== 'running');
      downloadBtn.setDisabled(status === 'running');
    });

    statstore.startUpdate();

    Ext.apply(me, {
      title: "Task viewer: " + task.desc + me.extraTitle,
      width: 800,
      height: 500,
      layout: 'fit',
      modal: true,
      items: [
        {
          xtype: 'tabpanel',
          region: 'center',
          items: [logView, statgrid]
        }
      ]
    });

    me.callParent();
    logView.fireEvent('show', logView);
  }
});
