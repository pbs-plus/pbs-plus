Ext.define('PBS.D2DManagement.BackupJobEdit', {
    extend: 'Proxmox.window.Edit',
    alias: 'widget.pbsDiskBackupJobEdit',
    mixins: ['Proxmox.Mixin.CBind'],

    userid: undefined,

    isAdd: true,

    subject: gettext('Disk Backup Job'),

    fieldDefaults: { labelWidth: 120 },

    bodyPadding: 0,

    cbindData: function(initialConfig) {
	let me = this;

	let baseurl = '/api2/extjs/config/disk-backup-job';
	let id = initialConfig.id;

	me.isCreate = !id;
	me.url = id ? `${baseurl}/${id}` : baseurl;
	me.method = id ? 'PUT' : 'POST';
	me.autoLoad = !!id;
	me.scheduleValue = id ? null : 'daily';
	me.authid = id ? null : Proxmox.UserName;
	me.editDatastore = me.datastore === undefined && me.isCreate;
	return { };
    },

    viewModel: {
    },

    initComponent: function() {
	let me = this;
	me.callParent();
    },

    items: {
	xtype: 'tabpanel',
	bodyPadding: 10,
	border: 0,
	items: [
	    {
		title: gettext('Options'),
		xtype: 'inputpanel',
		onGetValues: function(values) {
		    let me = this;

		    if (me.isCreate) {
			delete values.delete;
		    }

		    return values;
		},
		cbind: {
		    isCreate: '{isCreate}', // pass it through
		},
		column1: [
		    {
			xtype: 'pmxDisplayEditField',
			name: 'id',
			fieldLabel: gettext('Job ID'),
			renderer: Ext.htmlEncode,
			allowBlank: false,
			cbind: {
			    editable: '{isCreate}',
			},
		    },
		    {
			xtype: 'pbsD2DTargetSelector',
			fieldLabel: 'Target',
			name: 'target',
		    },
		    {
			xtype: 'pbsDataStoreSelector',
			fieldLabel: gettext('Local Datastore'),
			name: 'store',
		    listeners: {
			change: function(_, value) {
			    let me = this;
			    if (value) {
				let namespace = me.up('window').down('pbsNamespaceSelector');
				namespace.setDatastore(value);
				namespace.setDisabled(false);
			    }
			},
		    },
		    },
        {
			xtype: 'proxmoxtextfield',
      fieldLabel: gettext('Namespace'),
      disabled: true,
      name: 'ns',
        },
		],

		column2: [
		    {
			fieldLabel: gettext('Schedule'),
			xtype: 'pbsCalendarEvent',
			name: 'schedule',
			emptyText: gettext('none (disabled)'),
			cbind: {
			    deleteEmpty: '{!isCreate}',
			    value: '{scheduleValue}',
			},
		    },
		],

		columnB: [
		    {
			fieldLabel: gettext('Comment'),
			xtype: 'proxmoxtextfield',
			name: 'comment',
			cbind: {
			    deleteEmpty: '{!isCreate}',
			},
		    },
{
	xtype: 'textarea',
	name: 'rawexclusions',
	height: '100%',
	fieldLabel: gettext('Exclusions'),
	value: '',
	emptyText: gettext('Newline delimited list of exclusions following the .pxarexclude patterns.'),
    },
		],
	    },
	],
    },
});

