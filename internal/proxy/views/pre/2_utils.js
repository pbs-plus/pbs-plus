Ext.define('PBS.PlusUtils', {
  singleton: true,

  API2Request: function(reqOpts) {
    var newopts = Ext.apply({
      withCredentials: true,
      cors: true,
      useDefaultXhrHeader: false,
      waitMsg: gettext('Please wait...'),
    }, reqOpts);

    // default to enable if user isn't handling the failure already explicitly
    let autoErrorAlert =
      reqOpts.autoErrorAlert ??
      (typeof reqOpts.failure !== 'function' && typeof reqOpts.callback !== 'function');

    if (!newopts.url.match(/^\/api2/) && !newopts.url.match(/^[a-z][a-z\d+\-.]*:/i)) {
      newopts.url = '/api2/extjs' + newopts.url;
    }
    delete newopts.callback;
    let unmask = (target) => {
      if (target.waitMsgTargetCount === undefined || --target.waitMsgTargetCount <= 0) {
        target.setLoading(false);
        delete target.waitMsgTargetCount;
      }
    };

    let createWrapper = function(successFn, callbackFn, failureFn) {
      Ext.apply(newopts, {
        success: function(response, options) {
          if (options.waitMsgTarget) {
            if (Proxmox.Utils.toolkit === 'touch') {
              options.waitMsgTarget.setMasked(false);
            } else {
              unmask(options.waitMsgTarget);
            }
          }
          let result = Ext.decode(response.responseText);
          response.result = result;
          if (!result.success) {
            response.htmlStatus = Proxmox.Utils.extractRequestError(result, true);
            Ext.callback(callbackFn, options.scope, [options, false, response]);
            Ext.callback(failureFn, options.scope, [response, options]);
            if (autoErrorAlert) {
              Ext.Msg.alert(gettext('Error'), response.htmlStatus);
            }
            return;
          }
          Ext.callback(callbackFn, options.scope, [options, true, response]);
          Ext.callback(successFn, options.scope, [response, options]);
        },
        failure: function(response, options) {
          if (options.waitMsgTarget) {
            if (Proxmox.Utils.toolkit === 'touch') {
              options.waitMsgTarget.setMasked(false);
            } else {
              unmask(options.waitMsgTarget);
            }
          }
          response.result = {};
          try {
            response.result = Ext.decode(response.responseText);
          } catch (_e) {
            // ignore
          }
          let msg = gettext('Connection error') + ' - server offline?';
          if (response.aborted) {
            msg = gettext('Connection error') + ' - aborted.';
          } else if (response.timedout) {
            msg = gettext('Connection error') + ' - Timeout.';
          } else if (response.status && response.statusText) {
            msg =
              gettext('Connection error') +
              ' ' +
              response.status +
              ': ' +
              response.statusText;
          }
          response.htmlStatus = Ext.htmlEncode(msg);
          Ext.callback(callbackFn, options.scope, [options, false, response]);
          Ext.callback(failureFn, options.scope, [response, options]);
        },
      });
    };

    createWrapper(reqOpts.success, reqOpts.callback, reqOpts.failure);

    let target = newopts.waitMsgTarget;
    if (target) {
      if (Proxmox.Utils.toolkit === 'touch') {
        target.setMasked({ xtype: 'loadmask', message: newopts.waitMsg });
      } else if (target.rendered) {
        target.waitMsgTargetCount = (target.waitMsgTargetCount ?? 0) + 1;
        target.setLoading(newopts.waitMsg);
      } else {
        target.waitMsgTargetCount = (target.waitMsgTargetCount ?? 0) + 1;
        target.on(
          'afterlayout',
          function() {
            if ((target.waitMsgTargetCount ?? 0) > 0) {
              target.setLoading(newopts.waitMsg);
            }
          },
          target,
          { single: true },
        );
      }
    }
    Ext.Ajax.request(newopts);
  },

  render_task_status: function(value, metadata, record, rowIndex, colIndex, store) {
    if (
      !record.data['last-run-upid'] &&
      !store.getById('last-run-upid')?.data.value &&
      !record.data.upid &&
      !store.getById('upid')?.data.value
    ) {
      return '-';
    }

    if (!record.data['last-run-endtime'] && !store.getById('last-run-endtime')?.data.value) {
      metadata.tdCls = 'x-grid-row-loading';
      return '';
    }

    let parse_task_status = function(status) {
      if (status === 'OK') {
        return 'ok';
      }

      if (status === 'unknown') {
        return 'unknown';
      }

      let match = status.match(/^WARNINGS: (.*)$/);
      if (match) {
        return 'warning';
      }

      match = status.match(/^QUEUED: (.*)$/);
      if (match) {
        return 'queued';
      }

      return 'error';
    }

    let parsed = parse_task_status(value);
    let text = value;
    let icon = '';
    switch (parsed) {
      case 'unknown':
        icon = 'question faded';
        text = Proxmox.Utils.unknownText;
        break;
      case 'error':
        icon = 'times critical';
        text = Proxmox.Utils.errorText + ': ' + value;
        break;
      case 'warning':
        icon = 'exclamation warning';
        break;
      case 'ok':
        icon = 'check good';
        text = gettext("OK");
        break;
      case 'queued':
        icon = 'tasks faded';
        break;
    }

    return `<i class="fa fa-${icon}"></i> ${text}`;
  },
});

Ext.define(
  'PBS.PlusRestProxy',
  {
    extend: 'Proxmox.RestProxy',
    alias: 'proxy.pbsplus',

    // Inherit all the original properties
    pageParam: null,
    startParam: null,
    limitParam: null,
    groupParam: null,
    sortParam: null,
    filterParam: null,
    noCache: false,

    // Keep the original afterRequest method
    afterRequest: function(request, success) {
      this.fireEvent('afterload', this, request, success);
    },

    constructor: function(config) {
      // Add CORS credentials configuration for cross-origin requests
      config = Ext.apply({
        withCredentials: true,
        cors: true,
        useDefaultXhrHeader: false
      }, config);

      // Apply the original reader configuration
      Ext.applyIf(config, {
        reader: {
          responseType: undefined,
          type: 'json',
          rootProperty: config.root || 'data',
        },
      });

      this.callParent([config]);
    },
  }
);
