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

Ext.define("PBS.D2DManagement.BackupJobEdit", {
  extend: "Proxmox.window.Edit",
  alias: "widget.pbsDiskBackupJobEdit",
  mixins: ["Proxmox.Mixin.CBind"],

  userid: undefined,

  isAdd: true,

  subject: gettext("Disk Backup Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  cbindData: function (initialConfig) {
    let me = this;

    let baseurl = pbsPlusBaseUrl + "/api2/extjs/config/disk-backup-job";
    let id = initialConfig.id;

    me.isCreate = !id;
    me.url = id ? `${baseurl}/${encodePathValue(id)}` : baseurl;
    me.method = id ? "PUT" : "POST";
    me.autoLoad = !!id;
    me.scheduleValue = id ? null : "";
    me.authid = id ? null : Proxmox.UserName;
    me.editDatastore = me.datastore === undefined && me.isCreate;
    return {};
  },

  viewModel: {
    data: {
      storeValue: null, // or a default value for the datastore, e.g., "localDataStore"
      modeValue: "metadata",
      sourceModeValue: "snapshot",
    },
  },

  initComponent: function () {
    let me = this;
    me.callParent();

    if (me.jobData) {
      let inputPanel = me.down("inputpanel");
      if (inputPanel && inputPanel.setValues) {
        inputPanel.setValues(me.jobData);
      }

      if (me.jobData.store) {
        me.getViewModel().set("storeValue", me.jobData.store);
      }

      if (me.jobData.mode) {
        me.getViewModel().set("modeValue", me.jobData.mode);
      }

      if (me.jobData.sourcemode) {
        me.getViewModel().set("sourceModeValue", me.jobData.sourcemode);
      }
    }
  },

  items: {
    xtype: "tabpanel",
    bodyPadding: 10,
    border: 0,
    items: [
      {
        title: gettext("Options"),
        xtype: "inputpanel",
        onGetValues: function (values) {
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
            bind: {
              value: "{storeValue}",
            },
          },
          {
            xtype: "pbsD2DNamespaceSelector",
            fieldLabel: gettext("Namespace"),
            emptyText: gettext("Root"),
            name: "ns",
            bind: {
              datastore: "{storeValue}",
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
            xtype: "combo",
            fieldLabel: gettext("Backup Mode"),
            name: "mode",
            queryMode: "local",
            store: backupModes,
            displayField: "display",
            valueField: "value",
            bind: {
              value: "{modeValue}",
            },
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
          },
          {
            xtype: "combo",
            fieldLabel: gettext("Source Mode"),
            name: "sourcemode",
            queryMode: "local",
            store: sourceModes,
            displayField: "display",
            valueField: "value",
            bind: {
              value: "{sourceModeValue}",
            },
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
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
            height: "100%",
            fieldLabel: gettext("Exclusions"),
            value: "",
            emptyText: gettext(
              "Newline delimited list of exclusions following the .pxarexclude patterns.",
            ),
          },
        ],
      },
    ],
  },
});
