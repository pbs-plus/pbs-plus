Ext.define("PBS.MtfManagement", {
  extend: "Ext.tab.Panel",
  alias: "widget.pbsMtfManagement",

  title: "MTF Tapes",

  border: true,
  defaults: {
    border: false,
    xtype: "panel",
  },

  items: [
    {
      xtype: "pbsMtfCartridgePanel",
      title: gettext("Cartridges / Media Sets"),
      itemId: "mtf-inventory",
      iconCls: "fa fa-archive",
    },
    {
      xtype: "pbsMtfJobView",
      title: gettext("Migration Jobs"),
      itemId: "mtf-jobs",
      iconCls: "fa fa-floppy-o",
    },
    {
      xtype: "pbsMtfMappingPanel",
      title: gettext("Namespace Mappings"),
      itemId: "mtf-mappings",
      iconCls: "fa fa-sitemap",
    },
    {
      xtype: "pbsMtfChangerPanel",
      title: gettext("Changers / Drives"),
      itemId: "mtf-devices",
      iconCls: "fa fa-cogs",
    },
  ],
});
