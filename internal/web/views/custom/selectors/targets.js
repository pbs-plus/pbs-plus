Ext.define("PBS.form.D2DTargetSelector", {
  extend: "Proxmox.form.ComboGrid",
  alias: "widget.pbsD2DTargetSelector",

  editable: true,
  forceSelection: true,
  queryMode: "local",
  minChars: 1,
  filterPickList: true,
  typeAhead: false,

  allowBlank: false,
  autoSelect: false,

  displayField: "name",
  valueField: "name",
  value: null,

  store: {
    proxy: {
      type: "pbsplus",
      url: pbsPlusBaseUrl + "/api2/json/d2d/target",
    },
    autoLoad: true,
    sorters: "name",
  },

  listConfig: {
    width: 450,
    columns: [
      {
        text: gettext("Name"),
        dataIndex: "name",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "Agent Host",
        dataIndex: "agent_hostname",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: "Path",
        dataIndex: "path",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
    ],
  },

  initComponent: function () {
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
