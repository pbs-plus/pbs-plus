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
    model: "pbs-model-targets",
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
        text: gettext("Name"),
        dataIndex: "name",
        sortable: true,
        flex: 3,
        renderer: Ext.String.htmlEncode,
      },
      {
        text: gettext("Type"),
        dataIndex: "target_type",
        sortable: true,
        flex: 1,
        renderer: function (value) {
          let icons = {
            local: '<i class="fa fa-desktop"></i> Local',
            agent: '<i class="fa fa-server"></i> Agent',
            s3: '<i class="fa fa-cloud"></i> S3',
          };
          return icons[value] || Ext.String.htmlEncode(value);
        },
      },
      {
        text: gettext("Agent Host"),
        dataIndex: "agent_hostname",
        sortable: true,
        flex: 2,
        renderer: function (value) {
          return value ? Ext.String.htmlEncode(value) : "-";
        },
      },
      {
        text: gettext("Path / Volume"),
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
        text: gettext("Status"),
        dataIndex: "connection_status",
        sortable: true,
        flex: 1,
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
