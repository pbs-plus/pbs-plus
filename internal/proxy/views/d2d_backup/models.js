Ext.define('pbs-disk-backup-job-status', {
    extend: 'Ext.data.Model',
    fields: [
	'id', 'store', 'target', 'subpath', 'schedule', 'comment', 'duration',
	'next-run', 'last-run-upid', 'last-run-state', 'last-run-endtime', 'rawexclusions',
    ],
    idProperty: 'id',
    proxy: {
	type: 'proxmox',
	url: pbsPlusBaseUrl + '/api2/json/d2d/backup',
    },
});

Ext.define('pbs-model-targets', {
    extend: 'Ext.data.Model',
    fields: [
	'name', 'path',
    ],
    idProperty: 'name',
});

Ext.define('pbs-model-exclusions', {
    extend: 'Ext.data.Model',
    fields: [
	'path', 'comment',
    ],
    idProperty: 'path',
});

Ext.define('pbs-model-partial-files', {
    extend: 'Ext.data.Model',
    fields: [
      'path', 'comment',
    ],
    idProperty: 'path',
});
