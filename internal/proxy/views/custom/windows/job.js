var backupModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Metadata", value: "metadata" },
    { display: "Data", value: "data" },
    { display: "Legacy", value: "legacy" },
  ],
});

var sourceModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Snapshot", value: "snapshot" },
    { display: "Direct", value: "direct" },
  ],
});

var readModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Standard", value: "standard" },
    { display: "Memory Mapping", value: "mmap" },
  ],
});

Ext.define("PBS.D2DManagement.BackupJobEdit", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsDiskBackupJobEdit",
  mixins: ["Proxmox.Mixin.CBind"],

  userid: undefined,

  isAdd: true,

  subject: gettext("Disk Backup Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  cbindData: function(initialConfig) {
    let me = this;

    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/disk-backup-job";
    let id = initialConfig.id;

    me.isCreate = !id;
    me.url = id ? `${baseurl}/${encodeURIComponent(encodePathValue(id))}` : baseurl;
    me.method = id ? "PUT" : "POST";
    me.autoLoad = !!id;
    me.scheduleValue = id ? null : "";
    me.backupModeValue = id ? null : "metadata";
    me.sourceModeValue = id ? null : "snapshot";
    me.readModeValue = id ? null : "standard";
    me.authid = id ? null : Proxmox.UserName;
    me.editDatastore = me.datastore === undefined && me.isCreate;
    return {};
  },

  viewModel: {},

  controller: {
    xclass: "Ext.app.ViewController",
    control: {
      "pbsDataStoreSelector[name=store]": {
        change: "storeChange",
      },
    },

    storeChange: function(field, value) {
      let me = this;
      let nsSelector = me.lookup("namespace");
      nsSelector.setDatastore(value);
    },
  },

  initComponent: function() {
    let me = this;
    me.callParent();

    if (me.jobData) {
      let inputPanel = me.down("inputpanel");
      if (inputPanel && inputPanel.setValues) {
        inputPanel.setValues(me.jobData);
      }
    }
  },

  submit: function() {
    var me = this;
    var form = me.down('form').getForm();

    if (!form.isValid()) {
      return;
    }

    var values = form.getValues();

    Ext.Ajax.request({
      url: me.url,
      method: me.method,
      cors: true,
      withCredentials: true,
      useDefaultXhrHeader: false,
      params: values,
      success: function(response) {
        var result = Ext.decode(response.responseText);
        if (result.success) {
          me.close();
          if (me.listeners && me.listeners.destroy) {
            me.fireEvent('destroy');
          }
        } else {
          Ext.Msg.alert(gettext("Error"), result.message || "Unknown error");
        }
      },
      failure: function(response) {
        Ext.Msg.alert(gettext("Error"), response.statusText || "Request failed");
      }
    });
  },

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "inputpanel",
        onGetValues: function(values) {
          let me = this;

          if (me.isCreate) {
            delete values.delete;
          }

          return values;
        },
        cbind: {
          isCreate: "{isCreate}", // pass it through
        },
        column1: [
          {
            xtype: "pmxDisplayEditField",
            name: "id",
            fieldLabel: gettext("Job ID"),
            renderer: Ext.htmlEncode,
            allowBlank: true,
            cbind: {
              editable: "{isCreate}",
            },
          },
          {
            xtype: "pbsD2DTargetSelector",
            fieldLabel: "Target",
            name: "target",
          },
          {
            xtype: "proxmoxtextfield",
            fieldLabel: gettext("Subpath"),
            emptyText: gettext("/"),
            name: "subpath",
          },
          {
            xtype: "pbsDataStoreSelector",
            fieldLabel: gettext("Local Datastore"),
            name: "store",
          },
          {
            xtype: "pbsD2DNamespaceSelector",
            fieldLabel: gettext("Namespace"),
            emptyText: gettext("Root"),
            name: "ns",
            reference: "namespace",
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
          },
        ],

        column2: [
          {
            fieldLabel: gettext("Schedule"),
            xtype: "pbsD2DCalendarEvent",
            name: "schedule",
            emptyText: gettext("none (disabled)"),
            cbind: {
              deleteEmpty: "{!isCreate}",
              value: "{scheduleValue}",
            },
          },
          {
            xtype: "proxmoxtextfield",
            fieldLabel: gettext("Number of retries"),
            emptyText: gettext("0"),
            name: "retry",
          },
          {
            xtype: "proxmoxtextfield",
            fieldLabel: gettext("Retry interval (minutes)"),
            emptyText: gettext("1"),
            name: "retry-interval",
          },
          {
            xtype: "proxmoxtextfield",
            fieldLabel: gettext("Max number of entries per directory"),
            emptyText: gettext("1048576"),
            name: "max-dir-entries",
          },
          {
            xtype: "combo",
            fieldLabel: gettext("Backup Mode"),
            name: "mode",
            queryMode: "local",
            store: backupModes,
            displayField: "display",
            valueField: "value",
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
            cbind: {
              value: "{backupModeValue}",
            },
          },
          {
            xtype: "combo",
            fieldLabel: gettext("Source Mode"),
            name: "sourcemode",
            queryMode: "local",
            store: sourceModes,
            displayField: "display",
            valueField: "value",
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
            cbind: {
              value: "{sourceModeValue}",
            },
          },
          {
            xtype: "combo",
            fieldLabel: gettext("File Read Mode"),
            name: "readmode",
            queryMode: "local",
            store: readModes,
            displayField: "display",
            valueField: "value",
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
            cbind: {
              value: "{readModeValue}",
            },
          },
        ],

        columnB: [
          {
            fieldLabel: gettext("Comment"),
            xtype: "proxmoxtextfield",
            name: "comment",
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
          },
          {
            xtype: "textarea",
            name: "rawexclusions",
            height: 150,
            fieldLabel: gettext("Exclusions"),
            value: "",
            emptyText: gettext(
              "Newline delimited list of exclusions following the .pxarexclude patterns.",
            ),
          },
          {
            xtype: "pbsD2DScriptSelector",
            fieldLabel: "Pre-Backup Script",
            name: "pre_script",
          },
          {
            xtype: "pbsD2DScriptSelector",
            fieldLabel: "Post-Backup Script",
            name: "post_script",
          },
        ],
      },
    ],
  },
});
