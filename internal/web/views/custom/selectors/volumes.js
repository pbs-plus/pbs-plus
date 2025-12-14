Ext.define("PBS.form.D2DVolumeSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DVolumeSelector",

  editable: true,
  forceSelection: true,
  queryMode: "local",
  minChars: 1,
  filterPickList: true,
  typeAhead: false,

  allowBlank: false,
  autoSelect: false,

  displayField: "volume_name",
  valueField: "volume_name",
  value: null,

  store: {
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/volume/",
    },
    autoLoad: false,
    sorters: "volume_name",
  },

  listConfig: {
    width: 450,
    columns: [
      {
        text: gettext("Name"),
        dataIndex: "volume_name",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "Type",
        dataIndex: "meta_type",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "FS",
        dataIndex: "meta_fs",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
    ],
  },

  initComponent: function () {
    let me = this;

    if (me.changer) {
      me.store.getProxy().extraParams = { changer: me.changer };
    } else {
      me.store.getProxy().extraParams = {};
    }

    me.callParent();
  },

  setTarget: function (targetValue) {
    const me = this;
    const store = me.getStore();
    const proxy = store.getProxy();

    if (me.getValue()) {
      me.setValue(null);
    }

    if (!targetValue) {
      store.removeAll();
      return;
    }

    proxy.url =
      pbsPlusBaseUrl +
      "/api2/json/d2d/volume/" +
      encodeURIComponent(targetValue);

    store.load();
  },
});
