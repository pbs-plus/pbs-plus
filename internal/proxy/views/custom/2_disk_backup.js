Ext.define("PBS.D2DRestore", {
  extend: "Ext.tab.Panel",
  alias: "widget.pbsD2DRestore",

  title: "Disk Restore",

  tools: [],

  border: true,
  defaults: {
    border: false,
    xtype: "panel",
  },

  items: [
    {
      xtype: "pbsDiskBackupJobView",
      title: gettext("Backup Jobs"),
      itemId: "d2d-backup-jobs",
      iconCls: "fa fa-floppy-o",
    },
    {
      xtype: "pbsDiskTokenPanel",
      title: "Agent Bootstrap",
      itemId: "tokens",
      iconCls: "fa fa-handshake-o",
    },
    {
      xtype: "pbsDiskTargetPanel",
      title: "Targets",
      itemId: "targets",
      iconCls: "fa fa-desktop",
    },
    {
      xtype: "pbsDiskExclusionPanel",
      title: "Global Exclusions",
      itemId: "exclusions",
      iconCls: "fa fa-ban",
    },
    {
      xtype: "pbsDiskScriptPanel",
      title: "Scripts",
      itemId: "scripts",
      iconCls: "fa fa-file-code-o",
    },
  ],
});

