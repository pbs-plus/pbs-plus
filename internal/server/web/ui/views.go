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
		Name:   "PBS.D2DMounts",
		XType:  "pbsD2DMounts",
		Title:  "Mounts",
		Extend: js.ExtTabPanel,
		Border: true,
		Items: js.Items(
			js.Field{XType: "pbsPlusActiveMountsPanel", Title: "Active Mounts", ItemID: "d2d-mounts-active", IconCls: "fa fa-hdd-o"},
			js.Field{XType: "pbsPlusMountProfilesPanel", Title: "Mount Profiles", ItemID: "d2d-mounts-profiles", IconCls: "fa fa-cogs"},
		),
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
					{ text: "Mounts", iconCls: "fa fa-hdd-o", id: "d2d_mounts", path: "pbsD2DMounts", leaf: true },
					{ text: "Outposts", iconCls: "fa fa-globe", id: "d2d_outposts", path: "pbsPlusOutpostsPanel", leaf: true },
					{ text: "Data Verification", iconCls: "fa fa-check-circle", id: "data_verification", path: "pbsD2DDataVerification", leaf: true },
					{ text: "MTF Migration", iconCls: "fa fa-archive", id: "mtf_tapes", path: "pbsMtfManagement", leaf: true },
				],
			});
		}
	}
})`),
}
