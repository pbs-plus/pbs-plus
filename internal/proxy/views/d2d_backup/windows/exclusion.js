Ext.define('PBS.D2DManagement.ExclusionEditWindow', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsExclusionEditWindow',
    mixins: ['Proxmox.Mixin.CBind'],

    isCreate: true,
    isAdd: true,
    subject: 'Disk Backup Path Exclusion',
    cbindData: function(initialConfig) {
	let me = this;

	let contentid = initialConfig.contentid;
	let baseurl = '/api2/extjs/config/d2d-exclusion';

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
	    xtype: 'proxmoxcheckbox',
	    name: 'is_global',
	    fieldLabel: gettext('Apply to all jobs'),
      checked: true,
      defaultValue: true,
      uncheckedValue: false,
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
