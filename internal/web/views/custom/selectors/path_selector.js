Ext.define("PBS.form.D2DSnapshotPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DSnapshotPathSelector",

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
      handler: function (btn) {
        let me = btn.up("pbsD2DSnapshotPathSelector");

        if (!me.datastore || !me.snapshot) {
          Ext.Msg.alert(
            gettext("Error"),
            gettext("Please select a datastore and snapshot first."),
          );
          return;
        }

        let snapData = JSON.parse(me.snapshot)?.data;
        let listURL = `/api2/json/admin/datastore/${encodeURIComponent(me.datastore)}/catalog`;

        let archive =
          snapData.files.find((f) => f.filename.endsWith(".mpxar.didx")) ||
          snapData.files.find((f) => f.filename.endsWith(".pxar.didx"));

        if (!archive) {
          Ext.Msg.alert(
            gettext("Error"),
            gettext("No browsable archives found in this snapshot."),
          );
          return;
        }

        Ext.create("PBS.window.D2DPathSelector", {
          listURL: listURL,
          extraParams: {
            "backup-type": snapData.type,
            "backup-id": snapData.id,
            "backup-time": snapData.time,
            "archive-name": archive.filename,
            ns: me.ns || "",
          },
          listeners: {
            select: function (path) {
              me.lookupReference("pathField").setValue(path);
            },
          },
        }).show();
      },
    },
  ],

  setDatastore: function (ds) {
    console.log("PathSelector: setting datastore to", ds);
    this.datastore = ds;
  },
  setSnapshot: function (snap) {
    console.log("PathSelector: setting snapshot to", snap);
    this.snapshot = snap;
  },
  setNamespace: function (ns) {
    console.log("PathSelector: setting ns to", ns);
    this.ns = ns;
  },
});
