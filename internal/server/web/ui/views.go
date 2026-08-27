package ui

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var coreViews = []js.Value{
	js.Panel{
		Name:   "PBS.D2DManagement",
		XType:  "pbsD2DManagement",
		Title:  "Disk Backup",
		Extend: js.ExtTabPanel,
		Border: true,
		Items: js.Items(
			js.Field{XType: "pbsDiskBackupJobView", Title: "Backup Jobs", ItemID: "d2d-backup-jobs", IconCls: "fa fa-floppy-o"},
			js.Field{XType: "pbsDiskRestoreJobView", Title: "Restore Jobs", ItemID: "d2d-restore-jobs", IconCls: "fa fa-download"},
			js.Field{XType: "pbsDiskTokenPanel", Title: "Agent Bootstrap", ItemID: "tokens", IconCls: "fa fa-handshake-o"},
			js.Field{XType: "pbsDiskTargetPanel", Title: "Targets", ItemID: "targets", IconCls: "fa fa-desktop"},
			js.Field{XType: "pbsDiskExclusionPanel", Title: "Global Exclusions", ItemID: "exclusions", IconCls: "fa fa-ban"},
			js.Field{XType: "pbsDiskScriptPanel", Title: "Scripts", ItemID: "scripts", IconCls: "fa fa-file-code-o"},
			js.Field{XType: "pbsNotificationBatchView", Title: "Notification Batches", ItemID: "notification-batches", IconCls: "fa fa-bell-o"},
			js.Field{XType: "pbsD2DAlertSettings", Title: "Alert Settings", ItemID: "alert-settings", IconCls: "fa fa-exclamation-triangle"},
		),
		PanelDefaults: true,
	},
	js.Panel{
		Name:   "PBS.D2DSnapshotMount",
		Extend: js.ExtTabPanel,
		XType:  "pbsD2DSnapshotMount",
		Title:  "Snapshot Mount",
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
						tabs.push({ xtype: "pbsPlusActiveMountsPanel", title: "Active Mounts", itemId: "d2d-mounts-active", iconCls: "fa fa-hdd-o" });
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
	js.Panel{
		Name:   "PBS.D2DDataVerification",
		Extend: js.ExtTabPanel,
		XType:  "pbsD2DDataVerification",
		Title:  "Data Verification",
		Border: true,
		Methods: map[string]js.Raw{"initComponent": js.Func("", `
			var me = this;
			me.items = [{ xtype: "pbsVerificationJobPanel", title: "Verification Jobs", itemId: "d2d-verification-jobs", iconCls: "fa fa-check-circle" }];
			me.callParent();
		`)},
		PanelDefaults: true,
	},
	js.Panel{
		Name:   "PBS.MtfManagement",
		XType:  "pbsMtfManagement",
		Extend: js.ExtTabPanel,
		Title:  "MTF Tape Backup",
		Border: true,
		Items: js.Items(
			js.Field{XType: "pbsMtfInventoryPanel", Title: "Inventory", ItemID: "mtf-inventory", IconCls: "fa fa-book"},
			js.Field{XType: "pbsMtfChangerGrid", Title: "Changers", ItemID: "mtf-changers", IconCls: "fa fa-exchange"},
			js.Field{XType: "pbsMtfDriveGrid", Title: "Drives", ItemID: "mtf-drives", IconCls: "pbs-icon-tape-drive"},
			js.Field{XType: "pbsMtfMappingPanel", Title: "Namespace Mappings", ItemID: "mtf-mappings", IconCls: "fa fa-sitemap"},
			js.Field{XType: "pbsMtfJobView", Title: "Migration Jobs", ItemID: "mtf-jobs", IconCls: "fa fa-floppy-o"},
		),
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
