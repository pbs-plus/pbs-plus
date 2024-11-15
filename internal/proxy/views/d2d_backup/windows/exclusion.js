Ext.define('PBS.D2DManagement.ExclusionEditWindow', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsExclusionEditWindow',
    mixins: ['Proxmox.Mixin.CBind'],

    isCreate: true,
    isAdd: true,
    subject: 'D2D Backup Exclusion',
    cbindData: function(initialConfig) {
	let me = this;

	let driveid = initialConfig.driveid;
	let baseurl = '/api2/extjs/config/d2d-exclusion';

	me.isCreate = !driveid;
	me.url = driveid ? `${baseurl}/${encodeURIComponent(driveid)}` : baseurl;
	me.method = driveid ? 'PUT' : 'POST';

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
    ],
});
