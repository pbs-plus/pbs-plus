Ext.define("PBS.MtfManagement", {
  extend: "Ext.tab.Panel",
  alias: "widget.pbsMtfManagement",

  title: gettext("MTF Tape Backup"),

  border: true,
  defaults: {
    border: false,
    xtype: "panel",
  },

  items: [
    {
      xtype: "pbsMtfInventoryPanel",
      title: gettext("Inventory"),
      itemId: "mtf-inventory",
      iconCls: "fa fa-book",
    },
    {
      xtype: "pbsMtfChangerGrid",
      title: gettext("Changers"),
      itemId: "mtf-changers",
      iconCls: "fa fa-exchange",
    },
    {
      xtype: "pbsMtfDriveGrid",
      title: gettext("Drives"),
      itemId: "mtf-drives",
      iconCls: "pbs-icon-tape-drive",
    },
    {
      xtype: "pbsMtfMappingPanel",
      title: gettext("Namespace Mappings"),
      itemId: "mtf-mappings",
      iconCls: "fa fa-sitemap",
    },
    {
      xtype: "pbsMtfJobView",
      title: gettext("Migration Jobs"),
      itemId: "mtf-jobs",
      iconCls: "fa fa-floppy-o",
    },
  ],
});
