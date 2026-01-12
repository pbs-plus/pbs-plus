Ext.define("PBS.form.D2DSnapshotPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DSnapshotPathSelector",
  mixins: ["Proxmox.Mixin.CBind"],

  layout: "hbox",

  datastore: undefined,
  snapshot: undefined,
  ns: undefined,

  items: [
    {
      xtype: "proxmoxtextfield",
      name: "src-path",
      reference: "pathField",
      flex: 1,
      emptyText: gettext("/"),
      allowBlank: true,
    },
    {
      xtype: "button",
      iconCls: "fa fa-folder-open-o",
      margin: "0 0 0 5",
      handler: function () {
        let me = this.up("widget.pbsD2DSnapshotPathSelector");
        let ds = me.datastore;
        let snap = me.snapshot;

        if (!ds || !snap) {
          Ext.Msg.alert(
            gettext("Error"),
            gettext("Please select a datastore and snapshot first."),
          );
          return;
        }

        let listURL = `/api2/json/admin/datastore/${ds}/catalog`;

        Ext.create("PBS.window.PathSelector", {
          listURL: listURL,
          extraParams: {
            snapshot: snap,
            ns: me.ns || "",
          },
          listeners: {
            select: function (path) {
              me.lookup("pathField").setValue(path);
            },
          },
        }).show();
      },
    },
  ],

  setDatastore: function (ds) {
    this.datastore = ds;
  },
  setSnapshot: function (snap) {
    this.snapshot = snap;
  },
  setNamespace: function (ns) {
    this.ns = ns;
  },
});
