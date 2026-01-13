Ext.define("PBS.form.D2DSnapshotSelector", {
  extend: "Ext.form.field.ComboBox",
  alias: "widget.pbsD2DSnapshotSelector",

  config: {
    datastore: null,
    namespace: null,
  },

  allowBlank: false,
  autoSelect: true,
  valueField: "value",
  displayField: "display",
  emptyText: gettext("Select Snapshot"),
  editable: true,
  anyMatch: true,
  forceSelection: true,
  queryMode: "local",
  matchFieldWidth: false,
  listConfig: {
    minWidth: 170,
    maxWidth: 500,
    minHeight: 30,
    emptyText: `<div class="x-grid-empty">${gettext(
      "No snapshots accessible.",
    )}</div>`,
  },

  triggers: {
    clear: {
      cls: "pmx-clear-trigger",
      weight: -1,
      hidden: true,
      handler: function () {
        this.triggers.clear.setVisible(false);
        this.setValue("");
      },
    },
  },

  listeners: {
    change: function (field, value) {
      field.triggers.clear.setVisible(value !== "");
    },
  },

  initComponent: function () {
    let me = this;

    me.store = Ext.create("Ext.data.Store", {
      fields: [
        "backup-id",
        "backup-time",
        "backup-type",
        "files",
        {
          name: "value",
          convert: function (v, record) {
            return `${record.data["backup-type"] || "host"}/${record.data["backup-id"]}/${record.data["backup-time"]}`;
          },
        },
        {
          name: "display",
          convert: function (v, record) {
            let time = new Date(record.data["backup-time"] * 1000);
            let timeStr = Ext.Date.format(time, "Y-m-d H:i:s");
            return `${timeStr} | ${record.data["backup-id"]}`;
          },
        },
      ],
      autoLoad: !!me.getDatastore(),
      proxy: {
        type: "proxmox",
        timeout: 30 * 1000,
        url: me.getDatastore()
          ? `/api2/json/admin/datastore/${me.getDatastore()}/snapshots`
          : null,
        extraParams: {
          "backup-type": "host",
          ns: me.getNamespace() || null,
        },
      },
    });

    me.setDisabled(!me.getDatastore());

    me.callParent();
  },

  updateDatastore: function (newDatastore, oldDatastore) {
    let me = this;
    if (newDatastore) {
      me.setDisabled(false);
      let proxy = me.store.getProxy();
      proxy.setUrl(`/api2/json/admin/datastore/${newDatastore}/snapshots`);
      me.store.load();
      me.validate();
    } else {
      me.setDisabled(true);
      me.store.removeAll();
    }
  },

  updateNamespace: function (newNamespace, oldNamespace) {
    let me = this;
    if (me.getDatastore()) {
      let proxy = me.store.getProxy();
      proxy.setExtraParam("ns", newNamespace || null);
      me.store.load();
      me.validate();
    }
  },
});
