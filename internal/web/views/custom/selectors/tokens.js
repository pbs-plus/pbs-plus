Ext.define("PBS.form.D2DTokenSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DTokenSelector",

  allowBlank: false,
  autoSelect: false,

  displayField: "name",
  valueField: "name",
  value: null,

  store: {
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/token",
    },
    autoLoad: true,
    sorters: "name",
  },

  listConfig: {
    width: 450,
    columns: [
      {
        text: gettext("Token"),
        dataIndex: "token",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "Comment",
        dataIndex: "comment",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
    ],
  },

  initComponent: function() {
    let me = this;

    if (me.changer) {
      me.store.proxy.extraParams = {
        changer: me.changer,
      };
    } else {
      me.store.proxy.extraParams = {};
    }

    me.callParent();
  },
});
