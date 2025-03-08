Ext.define("PBS.form.D2DNamespaceSelector", {
  extend: "Ext.form.field.ComboBox",
  alias: "widget.pbsD2DNamespaceSelector",

  config: {
    datastore: null,
  },

  allowBlank: true,
  autoSelect: true,
  valueField: "ns",
  displayField: "ns",
  emptyText: gettext("Root"),
  editable: true,
  anyMatch: true,
  forceSelection: false, // allow custom values
  queryMode: "local",
  matchFieldWidth: false,
  listConfig: {
    minWidth: 170,
    maxWidth: 500,
    minHeight: 30,
    emptyText: `<div class="x-grid-empty">${gettext(
      "No namespaces accessible.",
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
      model: "pbs-namespaces",
      autoLoad: !!me.getDatastore(),
      filters: (rec) => rec.data.ns !== "",
      proxy: {
        type: "proxmox",
        timeout: 30 * 1000,
        // Use a default URL in case no datastore is provided.
        url: me.getDatastore()
          ? `/api2/json/admin/datastore/${me.getDatastore()}/namespace`
          : null,
      },
    });

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
        .setUrl(`/api2/json/admin/datastore/${newDatastore}/namespace`);
      this.store.load();
      this.validate();
    } else {
      this.setDisabled(true);
    }
  },
});
