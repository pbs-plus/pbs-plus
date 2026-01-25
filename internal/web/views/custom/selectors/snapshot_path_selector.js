Ext.define("PBS.form.D2DSnapshotPathSelector", {
  extend: "Ext.form.FieldContainer",
  alias: "widget.pbsD2DSnapshotPathSelector",

  layout: "hbox",

  deleteEmpty: false,

  datastore: undefined,
  snapshot: undefined,
  ns: undefined,

  initComponent: function () {
    let me = this;

    me.items = [
      {
        xtype: "proxmoxtextfield",
        name: "src-path",
        reference: "pathField",
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
          let me = btn.up("pbsD2DSnapshotPathSelector");

          let editWindow = btn.up("pbsDiskRestoreJobEdit");
          let snapSelector = editWindow.lookup("snapshot");
          let snapRecord = snapSelector.getSelection();

          if (!me.datastore || !me.snapshot || !snapRecord) {
            Ext.Msg.alert(
              gettext("Error"),
              gettext(
                "Please select a valid snapshot first. A snapshot with an ongoing backup is considered invalid.",
              ),
            );
            return;
          }

          let files = snapRecord.get("files");
          let archive =
            files.find((f) => f.filename.endsWith(".mpxar.didx")) ||
            files.find((f) => f.filename.endsWith(".pxar.didx"));

          if (!archive) {
            Ext.Msg.alert(
              gettext("Error"),
              gettext("No browsable archives found."),
            );
            return;
          }

          let parts = me.snapshot.split("/");

          Ext.create("PBS.window.D2DPathSelector", {
            listURL: `/api2/json/admin/datastore/${encodeURIComponent(me.datastore)}/catalog`,
            extraParams: {
              "backup-type": parts[0],
              "backup-id": parts[1],
              "backup-time": parts[2],
              "archive-name": archive.filename,
              ns: me.ns || "",
            },
            listeners: {
              select: function (path) {
                me.down("proxmoxtextfield[reference=pathField]").setValue(path);
              },
            },
          }).show();
        },
      },
    ];

    me.callParent();
  },

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
