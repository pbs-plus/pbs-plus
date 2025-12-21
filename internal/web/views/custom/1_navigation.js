Ext.onReady(function () {
  let store = Ext.getStore("NavigationStore");

  if (store) {
    let root = store.getRoot();

    let notesNode = root.findChild("path", "pbsTapeManagement", false);

    if (notesNode) {
      let index = root.indexOf(notesNode);

      root.insertChild(index, {
        text: "Disk Backup",
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
    }
  }
});
