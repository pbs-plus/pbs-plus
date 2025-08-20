Ext.onReady(function() {
  let store = Ext.getStore("NavigationStore");

  if (store) {
    let root = store.getRoot();

    let notesNode = root.findChild("path", "pbsTapeManagement", false);

    if (notesNode) {
      let index = root.indexOf(notesNode);

      root.insertChild(index - 1, {
        text: "Disk Backup",
        iconCls: "fa fa-hdd-o",
        id: "backup_targets",
        path: "pbsD2DManagement",
        expanded: true,
        children: [],
      });
    }
  }
});
