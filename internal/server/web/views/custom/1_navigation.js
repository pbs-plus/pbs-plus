Ext.onReady(function () {
  let store = Ext.getStore("NavigationStore");

  if (store) {
    let root = store.getRoot();

    let notesNode = root.findChild("path", "pbsTapeManagement", false);

    if (notesNode) {
      let index = root.indexOf(notesNode);

      root.insertChild(index, {
        text: "Disk Backup / Restore",
        iconCls: "fa fa-hdd-o",
        id: "backup_targets",
        path: "pbsD2DManagement",
        expanded: true,
        children: [],
      });
      root.insertChild(index + 1, {
        text: "Snapshot Mount",
        iconCls: "fa fa-hdd-o",
        id: "snapshot_mount",
        path: "pbsD2DSnapshotMount",
        expanded: true,
        children: [],
      });
      root.insertChild(index + 2, {
        text: "Data Verification",
        iconCls: "fa fa-check-circle",
        id: "data_verification",
        path: "pbsD2DDataVerification",
        expanded: true,
        children: [],
      });

      let ensureMtfNode = function () {
        let r = store.getRoot();
        if (!r) {
          return;
        }
        let tapeNode = r.findChild("path", "pbsTapeManagement", false);
        if (!tapeNode) {
          return;
        }
        if (tapeNode.findChild("path", "pbsMtfManagement", false)) {
          return;
        }
        tapeNode.appendChild({
          text: "MTF Migration",
          iconCls: "fa fa-archive",
          id: "mtf_tapes",
          path: "pbsMtfManagement",
          leaf: true,
        });
      };

      store.on("load", ensureMtfNode);
      ensureMtfNode();
      Ext.defer(ensureMtfNode, 500);
      Ext.defer(ensureMtfNode, 2000);
    }
  }
});
