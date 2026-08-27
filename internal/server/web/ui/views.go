package ui

import "github.com/pbs-plus/pbs-plus/internal/server/web/js"

var coreViews = []js.Value{
	js.TabPanel{
		Name:   "PBS.D2DManagement",
		XType:  "pbsD2DManagement",
		Border: true,
		Items: js.Arr{
			js.Obj{"xtype": "pbsDiskBackupJobView", "title": js.T("Backup Jobs"), "itemId": "d2d-backup-jobs", "iconCls": "fa fa-floppy-o"},
			js.Obj{"xtype": "pbsDiskRestoreJobView", "title": js.T("Restore Jobs"), "itemId": "d2d-restore-jobs", "iconCls": "fa fa-download"},
			js.Obj{"xtype": "pbsDiskTokenPanel", "title": "Agent Bootstrap", "itemId": "tokens", "iconCls": "fa fa-handshake-o"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": "Targets", "itemId": "targets", "iconCls": "fa fa-desktop"},
			js.Obj{"xtype": "pbsDiskExclusionPanel", "title": "Global Exclusions", "itemId": "exclusions", "iconCls": "fa fa-ban"},
			js.Obj{"xtype": "pbsDiskScriptPanel", "title": "Scripts", "itemId": "scripts", "iconCls": "fa fa-file-code-o"},
			js.Obj{"xtype": "pbsNotificationBatchView", "title": "Notification Batches", "itemId": "notification-batches", "iconCls": "fa fa-bell-o"},
			js.Obj{"xtype": "pbsD2DAlertSettings", "title": "Alert Settings", "itemId": "alert-settings", "iconCls": "fa fa-exclamation-triangle"},
		},
		PanelDefaults: true,
	},
	js.TabPanel{
		Name:   "PBS.D2DSnapshotMount",
		XType:  "pbsD2DSnapshotMount",
		Border: true,
		Methods: map[string]js.Raw{"initComponent": js.Func("", `
			var me = this;
			var store = Ext.data.StoreManager.lookup("pbs-datastore-list");
			if (!store) {
				Ext.log.warn("Store 'pbs-datastore-list' not found. Ensure it is created with a storeId before this component.");
			} else {
				store.load({ callback: function (records, operation, success) {
					if (success && records && records.length) {
						var tabs = [];
						Ext.Array.forEach(records, function (rec) {
							var name = rec.get("store");
							tabs.push({ xtype: "pbsPlusSnapshotMountDatastorePanel", title: name, itemId: "d2d-mount-" + name, iconCls: "fa fa-archive", datastore: name });
						});
						var added = me.add(tabs);
						if (added && added.length) {
							me.setActiveTab(added[0]);
						} else if (me.items && me.items.getCount() > 0) {
							me.setActiveTab(me.items.getAt(0));
						}
					}
				}});
			}
			me.callParent();
		`)},
		PanelDefaults: true,
	},
	js.TabPanel{
		Name:   "PBS.D2DDataVerification",
		XType:  "pbsD2DDataVerification",
		Border: true,
		Methods: map[string]js.Raw{"initComponent": js.Func("", `
			var me = this;
			me.items = [{ xtype: "pbsVerificationJobPanel", title: "Verification Jobs", itemId: "d2d-verification-jobs", iconCls: "fa fa-check-circle" }];
			me.callParent();
		`)},
		PanelDefaults: true,
	},
	js.TabPanel{
		Name:   "PBS.MtfManagement",
		XType:  "pbsMtfManagement",
		Title:  "MTF Tape Backup",
		Border: true,
		Items: js.Arr{
			js.Obj{"xtype": "pbsMtfInventoryPanel", "title": js.T("Inventory"), "itemId": "mtf-inventory", "iconCls": "fa fa-book"},
			js.Obj{"xtype": "pbsMtfChangerGrid", "title": js.T("Changers"), "itemId": "mtf-changers", "iconCls": "fa fa-exchange"},
			js.Obj{"xtype": "pbsMtfDriveGrid", "title": js.T("Drives"), "itemId": "mtf-drives", "iconCls": "pbs-icon-tape-drive"},
			js.Obj{"xtype": "pbsMtfMappingPanel", "title": js.T("Namespace Mappings"), "itemId": "mtf-mappings", "iconCls": "fa fa-sitemap"},
			js.Obj{"xtype": "pbsMtfJobView", "title": js.T("Migration Jobs"), "itemId": "mtf-jobs", "iconCls": "fa fa-floppy-o"},
		},
		PanelDefaults: true,
	},
	js.Raw(`Ext.onReady(function () {
	let store = Ext.getStore("NavigationStore");
	if (store) {
		let root = store.getRoot();
		let notesNode = root.findChild("path", "pbsTapeManagement", false);
		if (notesNode) {
			let index = root.indexOf(notesNode);
			root.insertChild(index, { text: "Disk Backup / Restore", iconCls: "fa fa-hdd-o", id: "backup_targets", path: "pbsD2DManagement", expanded: true, children: [] });
			root.insertChild(index + 1, { text: "Snapshot Mount", iconCls: "fa fa-hdd-o", id: "snapshot_mount", path: "pbsD2DSnapshotMount", expanded: true, children: [] });
			root.insertChild(index + 2, { text: "Data Verification", iconCls: "fa fa-check-circle", id: "data_verification", path: "pbsD2DDataVerification", expanded: true, children: [] });
			root.insertChild(index + 3, { text: "MTF Migration", iconCls: "fa fa-archive", id: "mtf_tapes", path: "pbsMtfManagement", leaf: true });
		}
	}
})`),
}
