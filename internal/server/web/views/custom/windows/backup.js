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
  data: [{ display: "Standard", value: "standard" }],
});

var xattrModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Include extra security attributes", value: "true" },
    { display: "Exclude extra security attributes", value: "false" },
  ],
});

var legacyXattrModes = Ext.create("Ext.data.Store", {
  fields: ["display", "value"],
  data: [
    { display: "Use broken xattr", value: "true" },
    { display: "Use fixed xattr", value: "false" },
  ],
});

Ext.define("PBS.D2DManagement.BackupJobInputPanel", {
  extend: "Proxmox.panel.InputPanel",
  xtype: "pbsD2DBackupInputPanel",

  setValues: function (values) {
    if (values.target && typeof values.target === "object") {
      values.target = values.target.name;
    }
    this.callParent([values]);
  },
});

Ext.define("PBS.D2DManagement.BackupJobEdit", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsDiskBackupJobEdit",
  mixins: ["Proxmox.Mixin.CBind"],

  userid: undefined,

  isAdd: true,

  subject: gettext("Disk Backup Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  cbindData: function (initialConfig) {
    let me = this;

    let baseurl = "/api2/extjs/config/disk-backup";
    let id = initialConfig.id;

    me.isCreate = !id;
    me.url = id
      ? `${baseurl}/${encodeURIComponent(encodePathValue(id))}`
      : baseurl;
    me.method = id ? "PUT" : "POST";
    me.autoLoad = !!id;
    me.scheduleValue = id ? null : "";
    me.backupModeValue = id ? null : "metadata";
    me.sourceModeValue = id ? null : "snapshot";
    me.readModeValue = id ? null : "standard";
    me.includeXAttrValue = id ? null : "true";
    me.legacyXAttrValue = id ? null : "false";
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
      "pbsD2DTargetSelector[name=target]": {
        change: "targetChange",
      },
    },

    storeChange: function (field, value) {
      let me = this;
      let nsSelector = me.lookup("namespace");
      nsSelector.setDatastore(value);
    },

    targetChange: function (field, value) {
      let me = this;
      let pathSel = me.lookup("pathSelectorSubpath");
      if (pathSel) {
        pathSel.setTarget(value);
      }
    },
  },

  initComponent: function () {
    let me = this;
    me.callParent();

    if (me.jobData) {
      let data = Ext.apply({}, me.jobData);
      if (data.target && typeof data.target === "object") {
        data.target = data.target.name;
      }
      me.setValues(data);
    }

    me.on("afterload", function (success, data) {
      if (success && data && data.target && typeof data.target === "object") {
        data.target = data.target.name;

        me.setValues(data);
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
        xtype: "pbsD2DBackupInputPanel",
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
            reference: "target",
          },
          {
            xtype: "pbsD2DTargetPathSelector",
            fieldLabel: gettext("Subpath"),
            reference: "pathSelectorSubpath",
            name: "subpath",
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
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
          {
            xtype: "combo",
            fieldLabel: gettext("Extra security attributes"),
            name: "include-xattr",
            queryMode: "local",
            store: xattrModes,
            displayField: "display",
            valueField: "value",
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
            cbind: {
              value: "{includeXAttrValue}",
            },
          },
          {
            xtype: "combo",
            fieldLabel: gettext("Xattr Migration"),
            name: "legacy-xattr",
            queryMode: "local",
            store: legacyXattrModes,
            displayField: "display",
            valueField: "value",
            editable: false,
            anyMatch: true,
            forceSelection: true,
            allowBlank: true,
            cbind: {
              value: "{legacyXAttrValue}",
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
