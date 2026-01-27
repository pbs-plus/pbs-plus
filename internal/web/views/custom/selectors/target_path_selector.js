Ext.define("PBS.form.D2DTargetPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DTargetPathSelector",

  layout: "hbox",
  target: undefined,

  targetLookup: "",
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
          let targetRecord = me.getSelection();

          if (!me.target || !targetRecord) {
            Ext.Msg.alert(
              gettext("Error"),
              gettext("Please select a target first."),
            );
            return;
          }

          Ext.create("PBS.window.D2DPathSelector", {
            listURL: `${pbsPlusBaseUrl}/api2/json/d2d/filetree/${encodeURIComponent(encodePathValue(me2.target))}`,
            prependSlash: false,
            onlyDirs: true,
            listeners: {
              select: function (path) {
                me2
                  .down("proxmoxtextfield[reference=destPathField]")
                  .setValue(path);
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
