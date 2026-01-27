Ext.define("PBS.form.D2DTargetPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DTargetPathSelector",

  layout: "hbox",
  target: undefined,

  onlyDirs: false,

  deleteEmpty: false,

  initComponent: function () {
    let me = this;

    me.items = [
      {
        xtype: "proxmoxtextfield",
        name: "dest-subpath",
        reference: "destPathField",
        flex: 1,
        emptyText: gettext("/"),
        allowBlank: true,
        deleteEmpty: me.deleteEmpty,
      },
      {
        xtype: "button",
        iconCls: "fa fa-folder-open-o",
        margin: "0 0 0 5",
        handler: function (btn) {
          if (!me.target) {
            Ext.Msg.alert(
              gettext("Error"),
              gettext("Please select a target first."),
            );
            return;
          }

          Ext.create("PBS.window.D2DPathSelector", {
            listURL: `${pbsPlusBaseUrl}/api2/json/d2d/filetree/${encodeURIComponent(encodePathValue(me.target))}`,
            prependSlash: false,
            onlyDirs: me.onlyDirs,
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
    ];

    me.callParent();
  },

  setTarget: function (target) {
    console.log("DestPathSelector: setting target to", target);
    this.target = target;
  },
});
