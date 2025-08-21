Ext.define("PBS.D2DRestore", {
  extend: "Ext.tab.Panel",
  alias: "widget.pbsD2DRestore",

  title: "Disk Restore",

  tools: [],

  border: true,
  defaults: {
    border: false,
    xtype: "panel",
  },

  initComponent: function() {
    let me = this;

    // Lookup datastore store
    let store = Ext.data.StoreManager.lookup("pbs-datastore-list");

    // Ensure store is loaded before creating tabs
    store.load({
      callback: function(records, operation, success) {
        if (success) {
          records.forEach((rec) => {
            me.add({
              xtype: "pbsPlusDatastorePanel",
              title: rec.get("store"),
              itemId: "d2d-restore-" + rec.get("store"),
              iconCls: "fa fa-floppy-o",
              cbind: {
                datastore: rec.get("store"),
              },
            });
          });

          // Activate the first tab automatically
          if (me.items.length > 0) {
            me.setActiveTab(0);
          }
        }
      },
    });

    me.callParent();
  },
});
