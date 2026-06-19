Ext.define("PBS.MtfManagement.MappingPanel", {
  extend: "Ext.grid.Panel",
  alias: "widget.pbsMtfMappingPanel",

  title: "Namespace Mappings",

  selType: "checkboxmodel",
  multiSelect: true,

  controller: {
    xclass: "Ext.app.ViewController",

    reload: function () {
      this.getView().getStore().load();
    },

    addMapping: function () {
      this.editMapping(null);
    },

    editMapping: function () {
      let recs = this.getView().getSelection();
      let id = recs.length === 1 ? recs[0].get("id") : null;
      this.openMappingWindow(id);
    },

    openMappingWindow: function (id) {
      let view = this.getView();
      Ext.create("Proxmox.window.Edit", {
        title: id ? gettext("Edit Mapping") : gettext("Add Mapping"),
        method: id ? "PUT" : "POST",
        isCreate: !id,
        autoShow: true,
        // Required by Proxmox.window.Edit initComponent; submitUrl overrides
        // the actual submit target.
        url: "/api2/extjs/config/mtf-mapping",
        submitUrl: function () {
          let base = "/api2/extjs/config/mtf-mapping";
          return id ? base + "/" + id : base;
        },
        items: {
          xtype: "inputpanel",
          column1: [
            {
              xtype: "textfield",
              name: "name",
              fieldLabel: gettext("Name"),
              allowBlank: false,
            },
            {
              xtype: "numberfield",
              name: "priority",
              fieldLabel: gettext("Priority"),
              value: 10,
              minValue: 0,
            },
            {
              xtype: "textfield",
              name: "match_regex",
              fieldLabel: gettext("Match Regex"),
              emptyText: '\\\\\\\\(?P<host>[^\\\\]+)\\\\(?P<drive>[A-Z]):',
            },
          ],
          column2: [
            {
              xtype: "textfield",
              name: "template",
              fieldLabel: gettext("Template"),
              allowBlank: false,
              emptyText: "{machine.short}/{drive}",
            },
            {
              xtype: "proxmoxcheckbox",
              name: "enabled",
              fieldLabel: gettext("Enabled"),
              value: true,
            },
            {
              xtype: "proxmoxcheckbox",
              name: "is_default",
              fieldLabel: gettext("Default Fallback"),
              value: false,
            },
          ],
          columnB: [
            {
              xtype: "displayfield",
              value:
                gettext(
                  "Templates support tokens {machine}, {machine.short}, {machine.label}, {drive}, {label} and regex captures $1..$N.",
                ),
              width: "100%",
            },
            {
              xtype: "textfield",
              name: "comment",
              fieldLabel: gettext("Comment"),
              width: "100%",
            },
          ],
        },
        listeners: {
          afterrender: function (win) {
            if (!id) return;
            PBS.PlusUtils.API2Request({
              url: "/api2/extjs/config/mtf-mapping/" + id,
              method: "GET",
              success: function (resp) {
                win.down("form").getForm().setValues(resp.result.data);
              },
              failure: function (resp) {
                Ext.Msg.alert(gettext("Error"), resp.htmlStatus);
                win.close();
              },
            });
          },
          destroy: () => view.getStore().load(),
        },
      }).show();
    },

    removeMappings: function () {
      let me = this;
      let recs = me.getView().getSelection();
      if (!recs.length) return;

      recs.forEach((r) => {
        PBS.PlusUtils.API2Request({
          url: "/api2/extjs/config/mtf-mapping/" + r.get("id"),
          method: "DELETE",
          success: () => me.reload(),
          failure: (resp) => Ext.Msg.alert(gettext("Error"), resp.htmlStatus),
        });
      });
    },

    renderEnabled: function (v) {
      return v
        ? '<i class="fa fa-check-circle" style="color:green"></i>'
        : '<i class="fa fa-times-circle" style="color:#888"></i>';
    },
  },

  listeners: {
    activate: function () {
      this.getStore().load();
    },
    itemdblclick: "editMapping",
  },

  store: {
    model: "pbs-mtf-mapping",
    autoLoad: true,
    proxy: {
      type: "ajax",
      url: pbsPlusBaseUrl + "/api2/extjs/config/mtf-mapping",
      reader: { type: "json", rootProperty: "data" },
    },
    sorters: [
      { property: "is_default", direction: "DESC" },
      { property: "priority", direction: "ASC" },
    ],
  },

  tbar: [
    {
      xtype: "proxmoxButton",
      text: gettext("Add Mapping"),
      selModel: false,
      handler: "addMapping",
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Edit"),
      handler: "editMapping",
      enableFn: function () {
        return this.up("grid").getSelection().length === 1;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Remove"),
      handler: "removeMappings",
      enableFn: function () {
        return this.up("grid").getSelection().length > 0;
      },
      disabled: true,
    },
    {
      xtype: "proxmoxButton",
      text: gettext("Reload"),
      selModel: false,
      handler: "reload",
    },
  ],

  columns: [
    {
      header: gettext("Default"),
      dataIndex: "is_default",
      width: 70,
      align: "center",
      renderer: function (v) {
        return v
          ? '<i class="fa fa-star" style="color:#f0ad4e"></i>'
          : "-";
      },
    },
    {
      header: gettext("Name"),
      dataIndex: "name",
      flex: 1.2,
      sortable: true,
    },
    {
      header: gettext("Pri"),
      dataIndex: "priority",
      width: 50,
      align: "right",
    },
    {
      header: gettext("Match Regex"),
      dataIndex: "match_regex",
      flex: 1.6,
      renderer: function (v) {
        if (!v)
          return '<span style="color:#888">' +
            gettext("(match all)") +
            "</span>";
        return "<code>" + Ext.String.htmlEncode(v) + "</code>";
      },
    },
    {
      header: gettext("Template"),
      dataIndex: "template",
      flex: 1.4,
      renderer: function (v) {
        return "<code>" + Ext.String.htmlEncode(v) + "</code>";
      },
    },
    {
      header: gettext("Enabled"),
      dataIndex: "enabled",
      width: 70,
      align: "center",
      renderer: "renderEnabled",
    },
  ],
});
