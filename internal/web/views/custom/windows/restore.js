Ext.define("PBS.D2DManagement.RestoreJobEdit", {
  extend: "PBS.plusWindow.Edit",
  alias: "widget.pbsDiskRestoreJobEdit",
  mixins: ["Proxmox.Mixin.CBind"],

  userid: undefined,

  isAdd: true,

  subject: gettext("Disk Restore Job"),

  fieldDefaults: { labelWidth: 120 },

  bodyPadding: 0,

  cbindData: function (initialConfig) {
    let me = this;

    let baseurl = "/api2/extjs/config/disk-restore";
    let id = initialConfig.id;

    me.isCreate = !id;
    me.url = id
      ? `${baseurl}/${encodeURIComponent(encodePathValue(id))}`
      : baseurl;
    me.method = id ? "PUT" : "POST";
    me.autoLoad = !!id;
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
      "pbsD2DNamespaceSelector[name=ns]": {
        change: "nsChange",
      },
      "pbsD2DSnapshotSelector[name=snapshot]": {
        change: "snapshotChange",
      },
    },

    storeChange: function (field, value) {
      let me = this;
      let nsSelector = me.lookup("namespace");
      let snapSelector = me.lookup("snapshot");

      nsSelector.setDatastore(value);
      snapSelector.setDatastore(value);

      let pathSel = me.lookup("pathSelector");
      if (pathSel) pathSel.setDatastore(value);

      if (field.isDirty()) {
        snapSelector.setValue(null);
      }
    },

    nsChange: function (field, value) {
      let me = this;
      let snapSelector = me.lookup("snapshot");

      snapSelector.setNamespace(value);

      let pathSel = me.lookup("pathSelector");
      if (pathSel) pathSel.setNamespace(value);

      if (field.isDirty()) {
        snapSelector.setValue(null);
      }
    },

    snapshotChange: function (field, value) {
      let me = this;
      let pathSel = me.lookup("pathSelector");
      if (pathSel) {
        pathSel.setSnapshot(value);
      }
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
            xtype: "pbsDataStoreSelector",
            fieldLabel: gettext("Local Datastore"),
            name: "store",
          },
          {
            xtype: "pbsD2DNamespaceSelector",
            fieldLabel: gettext("Snapshot namespace"),
            emptyText: gettext("Root"),
            name: "ns",
            reference: "namespace",
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
          },
          {
            xtype: "pbsD2DSnapshotSelector",
            fieldLabel: gettext("Snapshot"),
            name: "snapshot",
            reference: "snapshot",
            cbind: {
              deleteEmpty: "{!isCreate}",
            },
          },
          {
            xtype: "pbsD2DSnapshotPathSelector",
            fieldLabel: gettext("Path to restore"),
            reference: "pathSelector",
            name: "src-path",
          },
          {
            xtype: "pbsD2DTargetSelector",
            fieldLabel: "Target restore destination",
            name: "dest-target",
          },
          {
            xtype: "proxmoxtextfield",
            fieldLabel: gettext("Path to destination"),
            emptyText: gettext("/"),
            name: "dest-path",
          },
        ],

        column2: [
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
        ],
      },
    ],
  },
});
