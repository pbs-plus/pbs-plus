Ext.define("PBS.form.D2DSnapshotSelector", {
  extend: "Ext.form.field.ComboBox",
  alias: "widget.pbsD2DSnapshotSelector",

  config: {
    datastore: null,
    namespace: null,
  },

  allowBlank: false,
  autoSelect: true,
  valueField: "backup-id",
  displayField: "backup-id",
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
        "protected",
        "comment",
        "owner",
        "size",
      ],
      autoLoad: false,
      proxy: {
        type: "proxmox",
        timeout: 30 * 1000,
        // Use a default URL in case no datastore is provided.
        url: me.getDatastore()
          ? `/api2/json/admin/datastore/${me.getDatastore()}/snapshots`
          : null,
        extraParams: {
          "backup-type": "host",
        },
      },
    });

    // Set namespace if provided
    if (me.getNamespace()) {
      me.store.getProxy().setExtraParam("ns", me.getNamespace());
    }

    // Load if datastore is set
    if (me.getDatastore()) {
      me.store.load();
    }

    // disable or enable based on the datastore config
    me.setDisabled(!me.getDatastore());

    me.callParent();
  },

  updateDatastore: function (newDatastore, oldDatastore) {
    // When the datastore changes through binding, update the URL and reload
    if (newDatastore) {
      this.setDisabled(false);
      this.store
        .getProxy()
        .setUrl(`/api2/json/admin/datastore/${newDatastore}/snapshots`);
      this.store.load();
      this.validate();
    } else {
      this.setDisabled(true);
    }
  },

  updateNamespace: function (newNamespace, oldNamespace) {
    // When the namespace changes through binding, update the params and reload
    if (newNamespace) {
      this.store.getProxy().setExtraParam("ns", newNamespace);
    } else {
      this.store.getProxy().setExtraParam("ns", null);
    }
    if (this.getDatastore()) {
      this.store.load();
      this.validate();
    }
  },
});
