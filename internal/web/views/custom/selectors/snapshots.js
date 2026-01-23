Ext.define("PBS.form.D2DSnapshotSelector", {
  extend: "Ext.form.field.ComboBox",
  alias: "widget.pbsD2DSnapshotSelector",

  config: { datastore: null, namespace: null },

  valueField: "value",
  displayField: "display",
  queryMode: "local",
  anyMatch: true,
  forceSelection: false,
  autoSelect: false,

  initComponent: function () {
    let me = this;

    me.store = Ext.create("Ext.data.Store", {
      model: "pbs-model-d2d-snapshots",
      autoLoad: !!me.getDatastore(),
      sorters: [
        {
          property: "backup-time",
          direction: "DESC",
        },
      ],
      proxy: {
        type: "proxmox",
        url: me.getDatastore()
          ? `/api2/json/admin/datastore/${me.getDatastore()}/snapshots`
          : null,
        extraParams: { "backup-type": "host", ns: me.getNamespace() || null },
      },
      listeners: {
        load: function () {
          // This forces the UI to re-map the raw ID to the display date once loaded
          let val = me.getValue();
          if (val) me.setValue(val);
        },
      },
    });

    me.setDisabled(!me.getDatastore());
    me.callParent();
  },

  updateDatastore: function (newDatastore) {
    let me = this;
    if (newDatastore) {
      me.setDisabled(false);
      me.store
        .getProxy()
        .setUrl(`/api2/json/admin/datastore/${newDatastore}/snapshots`);
      me.store.load();
    } else {
      me.setDisabled(true);
      me.store.removeAll();
    }
  },

  updateNamespace: function (newNamespace) {
    let me = this;
    if (me.getDatastore()) {
      me.store.getProxy().setExtraParam("ns", newNamespace || null);
      me.store.load();
    }
  },
});
