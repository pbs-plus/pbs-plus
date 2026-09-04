package ui

import (
	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

var coreViews = []js.Value{
	js.Panel{
		Name:   "PBS.PlusConfiguration",
		XType:  "pbsPlusConfiguration",
		Title:  "PBS Plus Configuration",
		Extend: js.ExtTabPanel,
		Border: true,
		Items: js.Items(
			js.Field{XType: "pbsDiskExclusionPanel", Title: "Global Exclusions", ItemID: "exclusions", IconCls: "fa fa-ban"},
			js.Field{XType: "pbsDiskScriptPanel", Title: "Scripts", ItemID: "scripts", IconCls: "fa fa-file-code-o"},
			js.Field{XType: "pbsNotificationBatchView", Title: "Notification Batches", ItemID: "notification-batches", IconCls: "fa fa-bell-o"},
			js.Field{XType: "pbsD2DAlertSettings", Title: "Alert Settings", ItemID: "alert-settings", IconCls: "fa fa-exclamation-triangle"},
		),
		PanelDefaults: true,
	},
	js.Panel{
		Name:   "PBS.D2DManagement",
		XType:  "pbsD2DManagement",
		Title:  "Backup / Restore",
		Extend: js.ExtTabPanel,
		Border: true,
		Items: js.Items(
			js.Field{XType: "pbsDiskBackupJobView", Title: "Backup Jobs", ItemID: "d2d-backup-jobs", IconCls: "fa fa-floppy-o"},
			js.Field{XType: "pbsDiskRestoreJobView", Title: "Restore Jobs", ItemID: "d2d-restore-jobs", IconCls: "fa fa-download"},
		),
		PanelDefaults: true,
	},
	js.Panel{
		Name: "PBS.D2DTargets", XType: "pbsD2DTargets", Title: "Targets",
		Extend: js.ExtTabPanel, Border: true, PanelDefaults: true,
		Items: js.Items(
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("Filesystem"), "itemId": "filesystem-targets", "iconCls": "fa fa-folder", "targetKind": "filesystem", "stateId": "grid-filesystem-targets-v1"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("S3"), "itemId": "s3-targets", "iconCls": "fa fa-cloud", "targetKind": "s3", "stateId": "grid-s3-targets-v1"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("PostgreSQL"), "itemId": "postgresql-targets", "iconCls": "fa fa-database", "targetKind": "postgresql", "stateId": "grid-postgresql-targets-v1"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("MySQL / MariaDB"), "itemId": "mysql-targets", "iconCls": "fa fa-database", "targetKind": "mysql", "stateId": "grid-mysql-targets-v1"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("LDAP / Active Directory"), "itemId": "ldap-targets", "iconCls": "fa fa-sitemap", "targetKind": "ldap", "stateId": "grid-ldap-targets-v1"},
			js.Obj{"xtype": "pbsDiskTargetPanel", "title": js.T("Dovecot"), "itemId": "dovecot-targets", "iconCls": "fa fa-envelope", "targetKind": "dovecot", "stateId": "grid-dovecot-targets-v1"},
			js.Field{XType: "pbsDiskTokenPanel", Title: "Agent Bootstrap", ItemID: "tokens", IconCls: "fa fa-handshake-o"},
		),
	},
	js.Panel{
		Name:   "PBS.D2DSnapshotMount",
		Extend: js.ExtTabPanel,
		XType:  "pbsD2DSnapshotMount",
		Title:  "Snapshots",
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
						tabs.push({ xtype: "pbsPlusMountProfilesPanel", title: "Mount Profiles", itemId: "d2d-mounts-profiles", iconCls: "fa fa-cogs" });
						tabs.push({ xtype: "pbsPlusOutpostsPanel", title: "Outposts", itemId: "d2d-outposts", iconCls: "fa fa-globe" });
						Ext.Array.forEach(records, function (rec) {
							var name = rec.get("store");
							tabs.push({ xtype: "pbsPlusSnapshotMountDatastorePanel", title: name, itemId: "d2d-mount-" + name, iconCls: "fa fa-archive", datastore: name });
						});
						var added = me.add(tabs);
						if (added && added.length) {
							// land on the first datastore content tree, not Active Mounts
							me.setActiveTab(added[3] || added[0]);
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
		Title:  "MTF Migration",
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
			root.insertChild(index, {
				text: "PBS Plus",
				iconCls: "fa fa-plus-square",
				id: "pbs_plus",
				path: "pbsPlusConfiguration",
				expanded: true,
				children: [
					{ text: "Backup / Restore", iconCls: "fa fa-hdd-o", id: "backup_targets", path: "pbsD2DManagement", leaf: true },
					{ text: "Targets", iconCls: "fa fa-bullseye", id: "d2d_targets", path: "pbsD2DTargets", leaf: true },
					{ text: "Snapshots", iconCls: "fa fa-history", id: "snapshot_mount", path: "pbsD2DSnapshotMount", leaf: true },
					{ text: "Data Verification", iconCls: "fa fa-check-circle", id: "data_verification", path: "pbsD2DDataVerification", leaf: true },
					{ text: "MTF Migration", iconCls: "fa fa-archive", id: "mtf_tapes", path: "pbsMtfManagement", leaf: true },
				],
			});
		}
	}
})`),
}
