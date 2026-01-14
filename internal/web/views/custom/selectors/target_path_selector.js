Ext.define("PBS.form.D2DTargetPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DTargetPathSelector",

  layout: "hbox",

  target: undefined,

  items: [
    {
      xtype: "proxmoxtextfield",
      name: "dest-path",
      reference: "destPathField",
      flex: 1,
      emptyText: gettext("/"),
      allowBlank: true,
    },
    {
      xtype: "button",
      iconCls: "fa fa-folder-open-o",
      margin: "0 0 0 5",
      handler: function (btn) {
        let me = btn.up("pbsD2DTargetPathSelector");

        let editWindow = btn.up("pbsDiskRestoreJobEdit");
        let targetSelector = editWindow.lookup("dest-target");
        let targetRecord = targetSelector.getSelection(); // Gets the actual store record

        if (!me.target || !targetRecord) {
          Ext.Msg.alert(
            gettext("Error"),
            gettext("Please select a target first."),
          );
          return;
        }

        Ext.create("PBS.window.D2DPathSelector", {
          listURL: `${pbsPlusBaseUrl}/api2/json/d2d/filetree/${encodeURIComponent(encodePathValue(me.target))}`,
          prependSlash: false,
          onlyDirs: true,
          listeners: {
            select: function (path) {
              me.down("proxmoxtextfield[reference=destPathField]").setValue(
                path,
              );
            },
          },
        }).show();
      },
    },
  ],

  setTarget: function (target) {
    console.log("DestPathSelector: setting target to", target);
    this.target = target;
  },
});
