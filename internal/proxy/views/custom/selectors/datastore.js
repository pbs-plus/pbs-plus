Ext.define("PBS.form.D2DDataStoreSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DDataStoreSelector",

  allowBlank: false,
  autoSelect: false,
  valueField: "store",
  displayField: "store",

  store: {
    model: "pbs-datastore-list",
    autoLoad: true,
    sorters: "store",
  },

  initComponent: function () {
    let me = this;
    me.callParent();

    // Wait for the component to render, then add a listener on the store load event.
    // This ensures that once the datastore list is available, we update the combo's
    // value with the one from the view model.
    me.on("afterrender", function () {
      me.getStore().on(
        "load",
        function () {
          let wnd = me.up("window");
          if (wnd && wnd.getViewModel) {
            let vm = wnd.getViewModel();
            let val = vm.get("storeValue");
            if (val) {
              me.setValue(val);
            }
          }
        },
        me,
        { single: true },
      );
    });
  },

  listConfig: {
    columns: [
      {
        header: gettext("Datastore"),
        sortable: true,
        dataIndex: "store",
        renderer: (v, metaData, rec) => {
          let icon = "";
          if (rec.data?.maintenance) {
            let tip = Ext.String.htmlEncode(
              PBS.Utils.renderMaintenance(rec.data?.maintenance),
            );
            icon = ` <i data-qtip="${tip}" class="fa fa-wrench"></i>`;
          }
          return Ext.String.htmlEncode(v) + icon;
        },
        flex: 1,
      },
      {
        header: gettext("Comment"),
        sortable: true,
        dataIndex: "comment",
        renderer: Ext.String.htmlEncode,
        flex: 1,
      },
    ],
  },
});
