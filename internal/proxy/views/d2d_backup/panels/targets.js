Ext.define('PBS.D2DManagement.TargetPanel', {
    extend: 'Ext.grid.Panel',
    alias: 'widget.pbsDiskTargetPanel',

    controller: {
	xclass: 'Ext.app.ViewController',

	reloadTapeStore: function() {
	    let navtree = Ext.ComponentQuery.query('navigationtree')[0];
	    navtree.reloadTapeStore();
	},

	onAdd: function() {
	    let me = this;
	    Ext.create('PBS.D2DManagement.TargetEditWindow', {
		listeners: {
		    destroy: function() {
			me.reload();
		    },
		},
	    }).show();
	},

	onEdit: function() {
	    let me = this;
	    let view = me.getView();
	    let selection = view.getSelection();
	    if (!selection || selection.length < 1) {
		return;
	    }
	    Ext.create('PBS.D2DManagement.TargetEditWindow', {
		driveid: selection[0].data.name,
		autoLoad: true,
		listeners: {
		    destroy: () => me.reload(),
		},
	    }).show();
	},

	status: function(view, rI, cI, button, el, record) {
	    let me = this;
	    let drive = record.data.name;
	    PBS.Utils.driveCommand(drive, 'status', {
		waitMsgTarget: me.getView(),
		success: PBS.Utils.showDriveStatusWindow,
	    });
	},

	reload: function() {
	    this.getView().getStore().rstore.load();
	    this.reloadTapeStore();
	},

	stopStore: function() {
	    this.getView().getStore().rstore.stopUpdate();
	},

	startStore: function() {
	    this.getView().getStore().rstore.startUpdate();
	},

    render_status: function(value) {
      if (value.toString() == "true") {
        icon = 'check good';
        text = gettext("OK");
      } else {
        icon = 'times critical';
        text = "Unreachable";
      }

	return `<i class="fa fa-${icon}"></i> ${text}`;
    },

	init: function(view) {
	    Proxmox.Utils.monStoreErrors(view, view.getStore().rstore);
	},
    },

    listeners: {
	beforedestroy: 'stopStore',
	deactivate: 'stopStore',
	activate: 'startStore',
	itemdblclick: 'onEdit',
    },

    store: {
	type: 'diff',
	rstore: {
	    type: 'update',
	    storeid: 'proxmox-disk-targets',
	    model: 'pbs-model-targets',
	    proxy: {
		type: 'proxmox',
		url: "/api2/json/d2d/target",
	    },
	},
	sorters: 'name',
    },

    features: [
    ],

    tbar: [
	{
	    text: gettext('Add'),
	    xtype: 'proxmoxButton',
	    handler: 'onAdd',
	    selModel: false,
	},
	'-',
	{
	    text: gettext('Edit'),
	    xtype: 'proxmoxButton',
	    handler: 'onEdit',
	    disabled: true,
	},
	{
	    xtype: 'proxmoxStdRemoveButton',
	    baseurl: '/api2/extjs/config/d2d-target',
	    callback: 'reload',
	},
    ],
    columns: [
	{
	    text: gettext('Name'),
	    dataIndex: 'name',
	    flex: 1,
	},
	{
	    text: gettext('Path'),
	    dataIndex: 'path',
	    flex: 2,
	},
	{
	    header: gettext('Status'),
	    dataIndex: 'connection_status',
	    renderer: 'render_status',
	    flex: 3,
	},
    ],
});


