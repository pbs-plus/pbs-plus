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
        Ext.defer(function () {
          let r = Ext.getStore("NavigationStore").getRoot();
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
        }, 0);
      };

      ensureMtfNode();

      let hookTapeStore = function () {
        let nt = Ext.ComponentQuery.query("navigationtree")[0];
        if (nt && nt.tapeStore) {
          nt.tapeStore.on("load", ensureMtfNode);
          return true;
        }
        return false;
      };

      if (!hookTapeStore()) {
        Ext.defer(hookTapeStore, 2000);
      }
    }
  }
});
