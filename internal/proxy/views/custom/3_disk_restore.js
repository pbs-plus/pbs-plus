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
    var me = this;

    var store = Ext.data.StoreManager.lookup("pbs-datastore-list");
    if (!store) {
      Ext.log.warn(
        "Store 'pbs-datastore-list' not found. Ensure it is created with a storeId before this component."
      );
    } else {
      store.load({
        callback: function(records, operation, success) {
          if (success && records && records.length) {
            var tabs = [];
            Ext.Array.forEach(records, function(rec) {
              var name = rec.get("store");
              tabs.push({
                xtype: "pbsPlusDatastorePanel",
                title: name,
                itemId: "d2d-restore-" + name,
                iconCls: "fa fa-archive",
                datastore: name,
              });
            });

            // Add all tabs at once to minimize relayouts
            var added = me.add(tabs);

            // Activate first tab safely
            if (added && added.length) {
              me.setActiveTab(added[0]);
            } else if (me.items && me.items.getCount() > 0) {
              me.setActiveTab(me.items.getAt(0));
            }
          }
        },
      });
    }

    me.callParent();
  },
});
