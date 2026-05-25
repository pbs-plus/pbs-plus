Ext.define("PBS.D2DDataVerification", {
  extend: "Ext.tab.Panel",
  alias: "widget.pbsD2DDataVerification",

  title: "Data Verification",
  tools: [],
  border: true,

  defaults: {
    border: false,
    xtype: "panel",
  },

  initComponent: function () {
    var me = this;

    me.items = [
      {
        xtype: "pbsVerificationJobPanel",
        title: "Verification Jobs",
        itemId: "d2d-verification-jobs",
        iconCls: "fa fa-check-circle",
      },
    ];

    me.callParent();
  },
});
