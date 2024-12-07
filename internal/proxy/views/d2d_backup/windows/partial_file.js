Ext.define('PBS.D2DManagement.PartialFileEditWindow', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsPartialFileEditWindow',
    mixins: ['Proxmox.Mixin.CBind'],

    isCreate: true,
    isAdd: true,
    subject: 'Disk Backup Verify File Sizes',
    cbindData: function(initialConfig) {
	let me = this;

	let contentid = initialConfig.contentid;
	let baseurl = '/api2/extjs/config/d2d-partial-file';

	me.isCreate = !contentid;
	me.url = contentid ? `${baseurl}/${encodeURIComponent(contentid)}` : baseurl;
	me.method = contentid ? 'PUT' : 'POST';

	return { };
    },

    items: [
	{
	    fieldLabel: gettext('Path'),
	    name: 'path',
	    xtype: 'pmxDisplayEditField',
	    renderer: Ext.htmlEncode,
	    allowBlank: false,
	    cbind: {
		editable: '{isCreate}',
	    },
  },
		    {
			fieldLabel: gettext('Comment'),
			xtype: 'proxmoxtextfield',
			name: 'comment',
			cbind: {
			    deleteEmpty: '{!isCreate}',
			},
	},
    ],
});
