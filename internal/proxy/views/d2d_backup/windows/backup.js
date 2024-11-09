Ext.define('PBS.D2DManagement.BackupWindow', {
    extend: 'Proxmox.window.Edit',
    mixins: ['Proxmox.Mixin.CBind'],

    id: undefined,

    cbindData: function(config) {
	let me = this;
	return {
	    warning: Ext.String.format(gettext("Manually start backup job '{0}' ?"), me.id),
      id: me.id,
	};
    },

    title: gettext('Backup'),
    url: `/api2/extjs/d2d/backup`,
    showProgress: true,
    submitUrl: function(url, values) {
	let id = values.id;
	delete values.id;
	return `${url}/${id}`;
    },

    layout: 'hbox',
    width: 400,
    method: 'POST',
    isCreate: true,
    submitText: gettext('Ok'),
    items: [
	{
	    xtype: 'container',
	    padding: 0,
	    layout: {
		type: 'hbox',
		align: 'stretch',
	    },
	    items: [
		{
		    xtype: 'component',
		    cls: [Ext.baseCSSPrefix + 'message-box-icon',
			Ext.baseCSSPrefix + 'message-box-warning',
			Ext.baseCSSPrefix + 'dlg-icon'],
		},
		{
		    xtype: 'container',
		    flex: 1,
		    items: [
			{
			    xtype: 'displayfield',
			    cbind: {
				value: '{warning}',
			    },
			},
			{
			    xtype: 'hidden',
			    name: 'id',
			    cbind: {
				value: '{id}',
			    },
			},
		    ],
		},
	    ],
	},
    ],
});
