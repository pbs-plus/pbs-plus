Ext.define('PBS.PlusUtils', {
  singleton: true,

  API2Request: function(reqOpts) {
    var enhancedOpts = Ext.apply({
      withCredentials: true,
      cors: true,
      useDefaultXhrHeader: false
    }, reqOpts);

    if (enhancedOpts.url && enhancedOpts.url.charAt(0) === '/') {
      enhancedOpts.url = pbsPlusBaseUrl + enhancedOpts.url;
    }

    return Proxmox.Utils.API2Request(enhancedOpts);
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
