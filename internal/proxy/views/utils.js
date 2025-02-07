Ext.define('PBSPlus.Utils', {
  singleton: true,
  render_task_status: function(value, metadata, record, rowIndex, colIndex, store) {
    var lastPlusError = record.data['last-plus-error'] || store.getById('last-plus-error')?.data.value
    if (lastPlusError) {
      return `<i class="fa fa-times critical"></i> ${lastPlusError}`;
    }

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

	  let parsed = Proxmox.Utils.parse_task_status(value);
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
	  }

    return `<i class="fa fa-${icon}"></i> ${text}`;
  },
});
