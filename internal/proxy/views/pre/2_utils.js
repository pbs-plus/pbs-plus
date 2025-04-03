Ext.define('PBS.PlusUtils', {
  singleton: true,
  
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
