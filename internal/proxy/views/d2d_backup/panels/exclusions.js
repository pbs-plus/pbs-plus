Ext.define('PBS.D2DManagement.ExclusionPanel', {
    extend: 'Ext.grid.Panel',
    alias: 'widget.pbsDiskExclusionPanel',

    controller: {
	xclass: 'Ext.app.ViewController',

	onAdd: function() {
	    let me = this;
	    Ext.create('PBS.D2DManagement.ExclusionEditWindow', {
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
	    Ext.create('PBS.D2DManagement.ExclusionEditWindow', {
		driveid: selection[0].data.name,
		autoLoad: true,
		listeners: {
		    destroy: () => me.reload(),
		},
	    }).show();
	},

	reload: function() {
	    this.getView().getStore().rstore.load();
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
        text = "Applies to all jobs";
      } else {
        icon = 'times critical';
        text = "Not applied by default";
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
	    storeid: 'proxmox-disk-exclusions',
	    model: 'pbs-model-exclusions',
	    proxy: {
		type: 'proxmox',
		url: "/api2/json/d2d/exclusion",
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
	    baseurl: '/api2/extjs/config/d2d-exclusion',
	    callback: 'reload',
	},
    ],
    columns: [
	{
	    text: gettext('Path'),
	    dataIndex: 'path',
	    flex: 1,
	},
	{
	    text: gettext('Global'),
	    dataIndex: 'is_global',
	    renderer: 'render_status',
	    flex: 2,
	},
    ],
});


