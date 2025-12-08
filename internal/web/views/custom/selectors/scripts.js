Ext.define("PBS.form.D2DScriptSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DScriptSelector",

  allowBlank: true,
  autoSelect: false,

  displayField: "path",
  valueField: "path",
  value: null,

  store: {
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/script",
    },
    autoLoad: true,
    sorters: "path",
  },

  listConfig: {
    width: 450,
    columns: [
      {
        text: "Path",
        dataIndex: "path",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "Description",
        dataIndex: "description",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
    ],
  },

  initComponent: function() {
    let me = this;

    me.callParent();
  },
});
