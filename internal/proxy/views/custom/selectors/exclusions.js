Ext.define("PBS.form.D2DExclusionSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DExclusionSelector",

  allowBlank: false,
  autoSelect: false,

  displayField: "name",
  valueField: "name",
  value: null,

  store: {
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/exclusion",
    },
    autoLoad: true,
    sorters: "name",
  },

  listConfig: {
    width: 450,
    columns: [
      {
        text: gettext("Path"),
        dataIndex: "path",
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
