Ext.define('PBS.config.DiskBackupJobView', {
    extend: 'Ext.grid.GridPanel',
    alias: 'widget.pbsDiskBackupJobView',

    stateful: true,
    stateId: 'grid-disk-backup-jobs-v1',

    title: 'Disk Backup Jobs',

    controller: {
	xclass: 'Ext.app.ViewController',

	addJob: function() {
	    let me = this;
	    Ext.create('PBS.D2DManagement.BackupJobEdit', {
		autoShow: true,
		listeners: {
		    destroy: function() {
			me.reload();
		    },
		},
	    }).show();
	},

	editJob: function() {
	    let me = this;
	    let view = me.getView();
	    let selection = view.getSelection();
	    if (!selection || selection.length < 1) {
		return;
	    }

	    Ext.create('PBS.D2DManagement.BackupJobEdit', {
		id: selection[0].data.id,
		autoShow: true,
		listeners: {
		    destroy: function() {
			me.reload();
		    },
		},
	    }).show();
	},

	openTaskLog: function() {
	    let me = this;
	    let view = me.getView();
	    let selection = view.getSelection();
	    if (selection.length < 1) return;

	    let upid = selection[0].data['last-run-upid'];
	    if (!upid) return;

	    Ext.create('Proxmox.window.TaskViewer', {
		upid,
	    }).show();
	},

	runJob: function() {
	    let me = this;
	    let view = me.getView();
	    let selection = view.getSelection();
	    if (selection.length < 1) return;

	    let id = selection[0].data.id;

      Ext.create('PBS.D2DManagement.BackupWindow', {
		id,
		listeners: {
		    destroy: function() {
        me.reload()
		    },
		},
	    }).show();
	},

	startStore: function() { this.getView().getStore().rstore.startUpdate(); },

	stopStore: function() { this.getView().getStore().rstore.stopUpdate(); },

	reload: function() { this.getView().getStore().rstore.load(); },

	init: function(view) {
	    Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
	},
    },

    listeners: {
	activate: 'startStore',
	deactivate: 'stopStore',
	itemdblclick: 'editJob',
    },

    store: {
	type: 'diff',
	autoDestroy: true,
	autoDestroyRstore: true,
	sorters: 'id',
	rstore: {
	    type: 'update',
	    storeid: 'pbs-disk-backup-job-status',
	    model: 'pbs-disk-backup-job-status',
	    interval: 5000,
	},
    },

    viewConfig: {
	trackOver: false,
    },

    tbar: [
	{
	    xtype: 'proxmoxButton',
	    text: gettext('Add'),
	    selModel: false,
	    handler: 'addJob',
	},
	{
	    xtype: 'proxmoxButton',
	    text: gettext('Edit'),
	    handler: 'editJob',
	    disabled: true,
	},
	{
	    xtype: 'proxmoxStdRemoveButton',
	    baseurl: '/api2/extjs/config/disk-backup-job',
	    confirmMsg: gettext('Remove entry?'),
	    callback: 'reload',
	},
	'-',
	{
	    xtype: 'proxmoxButton',
	    text: gettext('Show Log'),
	    handler: 'openTaskLog',
	    enableFn: (rec) => !!rec.data['last-run-upid'],
	    disabled: true,
	},
	{
	    xtype: 'proxmoxButton',
	    text: gettext('Run now'),
	    handler: 'runJob',
      reference: 'd2dBackupRun',
	    disabled: true,
	},
    ],

    columns: [
	{
	    header: gettext('Job ID'),
	    dataIndex: 'id',
	    renderer: Ext.String.htmlEncode,
	    maxWidth: 220,
	    minWidth: 75,
	    flex: 1,
	    sortable: true,
	},
	{
	    header: gettext('Target'),
	    dataIndex: 'target',
	    width: 120,
	    sortable: true,
	},
	{
	    header: gettext('Subpath'),
	    dataIndex: 'subpath',
	    width: 120,
	    sortable: true,
	},
	{
	    header: gettext('Datastore'),
	    dataIndex: 'store',
	    width: 120,
	    sortable: true,
	},
	{
	    header: gettext('Schedule'),
	    dataIndex: 'schedule',
	    maxWidth: 220,
	    minWidth: 80,
	    flex: 1,
	    sortable: true,
	},
	{
	    header: gettext('Last Backup'),
	    dataIndex: 'last-run-endtime',
	    renderer: PBS.Utils.render_optional_timestamp,
	    width: 150,
	    sortable: true,
	},
	{
	    text: gettext('Duration'),
	    dataIndex: 'duration',
	    renderer: Proxmox.Utils.render_duration,
	    width: 80,
	},
	{
	    header: gettext('Status'),
	    dataIndex: 'last-run-state',
	    renderer: PBS.Utils.render_task_status,
	    flex: 3,
	},
	{
	    header: gettext('Next Run'),
	    dataIndex: 'next-run',
	    renderer: PBS.Utils.render_next_task_run,
	    width: 150,
	    sortable: true,
	},
	{
	    header: gettext('Comment'),
	    dataIndex: 'comment',
	    renderer: Ext.String.htmlEncode,
	    flex: 2,
	    sortable: true,
	},
    ],

    initComponent: function() {
	let me = this;

	me.callParent();
    },
});


