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
    width: 600,
    columns: [
      {
        header: gettext("Name"),
        dataIndex: "name",
        sortable: true,
        flex: 2,
        renderer: Ext.String.htmlEncode,
      },
      {
        header: gettext("Type"),
        dataIndex: "target_type",
        sortable: true,
        flex: 1,
        renderer: function (value) {
          let icons = {
            local: '<i class="fa fa-desktop"></i> Local',
            agent: '<i class="fa fa-server"></i> Agent',
            s3: '<i class="fa fa-cloud"></i> S3',
          };
          return icons[value] || Ext.String.htmlEncode(value || "");
        },
      },
      {
        header: gettext("Path / Volume"),
        dataIndex: "path",
        sortable: true,
        flex: 3,
        renderer: function (value, metaData, record) {
          if (record.get("target_type") === "agent") {
            let volumeName = record.get("volume_name");
            let volumeId = record.get("volume_id");
            return Ext.String.htmlEncode(volumeName || volumeId || "-");
          }
          return value ? Ext.String.htmlEncode(value) : "-";
        },
      },
      {
        header: gettext("Status"),
        dataIndex: "connection_status",
        sortable: true,
        width: 80,
        renderer: function (value) {
          if (value === true) {
            return '<i class="fa fa-check good"></i>';
          } else if (value === false) {
            return '<i class="fa fa-times critical"></i>';
          }
          return "-";
        },
      },
    ],
  },

  initComponent: function () {
    let me = this;

    me.store.proxy.extraParams = me.changer ? { changer: me.changer } : {};

    me.callParent();
  },
});
