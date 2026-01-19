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
      "pbsD2DTargetSelector[name=dest-target]": {
        change: "targetChange",
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

    targetChange: function (field, value) {
      let me = this;
      let pathSel = me.lookup("pathSelectorDestination");
      if (pathSel) {
        pathSel.setTarget(value);
      }
    },
  },

  initComponent: function () {
    let me = this;
    me.callParent();

    if (me.jobData) {
      let inputPanel = me.down("inputpanel");
      if (inputPanel && inputPanel.setValues) {
        let data = Ext.apply({}, me.jobData);
        if (data.target && typeof data.target === "object") {
          data.target = data.target.name;
        }
        inputPanel.setValues(data);
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
        ],

        column2: [
          {
            xtype: "pbsD2DTargetSelector",
            fieldLabel: "Target restore destination",
            name: "dest-target",
            reference: "dest-target",
          },
          {
            xtype: "pbsD2DTargetPathSelector",
            fieldLabel: gettext("Path to destination"),
            reference: "pathSelectorDestination",
            name: "dest-subpath",
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
            xtype: "pbsD2DScriptSelector",
            fieldLabel: "Pre-Restore Script",
            name: "pre_script",
          },
          {
            xtype: "pbsD2DScriptSelector",
            fieldLabel: "Post-Restore Script",
            name: "post_script",
          },
        ],
      },
    ],
  },
});
