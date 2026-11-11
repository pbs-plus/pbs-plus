package web

import (
	"bytes"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/web/ui"
)

func TestCustomJSMigratesDefinitionsOnce(t *testing.T) {
	source := ui.Render()

	for _, name := range [][]byte{
		[]byte(`Ext.define("pbs-model-scripts"`),
		[]byte(`Ext.define("pbs-mtf-job"`),
		[]byte(`Ext.define("pbs-mtf-family"`),
		[]byte(`Ext.define("pbs-mtf-cartridge"`),
		[]byte(`Ext.define("pbs-mtf-dataset"`),
		[]byte(`Ext.define("pbs-mtf-mapping"`),
		[]byte(`Ext.define("PBS.D2DManagement"`),
		[]byte(`Ext.define("PBS.D2DSnapshotMount"`),
		[]byte(`Ext.define("PBS.D2DDataVerification"`),
		[]byte(`Ext.define("PBS.MtfManagement"`),
		[]byte(`Ext.define("PBS.form.D2DExclusionSelector"`),
		[]byte(`Ext.define("PBS.form.D2DNamespaceSelector"`),
		[]byte(`Ext.define("PBS.form.D2DSnapshotSelector"`),
		[]byte(`Ext.define("PBS.form.D2DTargetSelector"`),
		[]byte(`Ext.define("PBS.form.D2DDatabaseClientSelector"`),
		[]byte(`Ext.define("PBS.form.D2DTokenSelector"`),
		[]byte(`Ext.define("PBS.form.D2DCalendarEvent"`),
		[]byte(`Ext.define("PBS.form.D2DTargetPathSelector"`),
		[]byte(`Ext.define("PBS.form.D2DSnapshotPathSelector"`),
		[]byte(`Ext.define("PBS.D2DManagement.ExclusionEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TokenEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetFilesystemEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetS3EditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetPostgreSQLEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetMySQLEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetS3Secret"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetDatabasePassword"`),
		[]byte(`Ext.define("PBS.D2DManagement.ExclusionPanel"`),
		[]byte(`Ext.define("PBS.MtfManagement.ChangerGrid"`),
		[]byte(`Ext.define("PBS.MtfManagement.DriveGrid"`),
		[]byte(`Ext.define("PBS.D2DManagement.NotificationBatchView"`),
		[]byte(`Ext.define("PBS.D2DManagement.TokenPanel"`),
		[]byte(`Ext.define("PBS.MtfManagement.JobView"`),
		[]byte(`Ext.define("PBS.MtfManagement.InventoryPanel"`),
		[]byte(`Ext.define("PBS.MtfManagement.MappingPanel"`),
		[]byte(`Ext.define("PBS.config.DiskRestoreJobView"`),
		[]byte(`Ext.define("PBS.config.DiskBackupJobView"`),
		[]byte(`Ext.define("PBS.D2DVerification.JobPanel"`),
		[]byte(`Ext.define("PBS.D2DSnapshotMount.DatastorePanel"`),
		[]byte(`Ext.define("pbs-model-active-mounts"`),
		[]byte(`Ext.define("PBS.D2DSnapshotMount.ActiveMountsPanel"`),
		[]byte(`Ext.define("pbs-model-mount-profiles"`),
		[]byte(`Ext.define("PBS.D2DSnapshotMount.ProfilesPanel"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetPanelController"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetPanel"`),
		[]byte(`Ext.define("PBS.D2DManagement.Alerts"`),
		[]byte(`Ext.define("PBS.D2DManagement.AlertEditWindow"`),
		[]byte(`PBS.D2DManagement.makeNotificationTab =`),
		[]byte(`Ext.define("PBS.D2DManagement.RestoreJobEdit"`),
		[]byte(`Ext.define("PBS.D2DManagement.BackupJobEdit"`),
		[]byte(`Ext.define("PBS.MtfManagement.JobEdit"`),
		[]byte(`Ext.define("PBS.D2DManagement.NotificationBatchEdit"`),
		[]byte(`Ext.define("PBS.window.D2DPathSelector"`),
		[]byte(`Ext.define("PBS.D2DVerification.FilterEditWindow"`),
		[]byte(`Ext.define("PBS.D2DVerification.OptionsInputPanel"`),
		[]byte(`Ext.define("PBS.D2DVerification.SpotCheckInputPanel"`),
		[]byte(`Ext.define("PBS.D2DVerification.JobEdit"`),
		[]byte("override: 'PBS.TapeManagement.SnapshotGrid'"),
		[]byte(`Ext.define("PBS.form.D2DScriptSelector"`),
		[]byte(`Ext.define("PBS.D2DManagement.ScriptEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.ScriptPanel"`),
		[]byte(`Ext.define("PBS.D2DTargets"`),
	} {
		if got := bytes.Count(source, name); got != 1 {
			t.Errorf("%s is defined %d times, want 1", name, got)
		}
	}
}

func TestCodeMirrorFieldUsesNativeFormValue(t *testing.T) {
	source := ui.Render()
	for _, marker := range []string{
		`xtype: "textarea",`,
		`name: "script",`,
		`CodeMirror.fromTextArea(component.inputEl.dom, {`,
		`component.setValue(value);`,
		`editor.setValue(value || "");`,
	} {
		if !bytes.Contains(source, []byte(marker)) {
			t.Errorf("rendered JS lost native code editor form integration: %s", marker)
		}
	}
	if bytes.Contains(source, []byte(`let pendingValue = "";`)) {
		t.Error("rendered JS still keeps script content outside the form")
	}
}

func TestCodeEditorAssetsAreServedLocally(t *testing.T) {
	source := compileJS(&preJsFS)
	if bytes.Contains(source, []byte("cdnjs.cloudflare.com")) {
		t.Error("script editor still loads assets from a public CDN")
	}
	for embedded, name := range vendoredAssets {
		data, err := vendorFS.ReadFile(embedded)
		if err != nil {
			t.Fatalf("vendored asset %s is missing: %v", embedded, err)
		}
		if len(data) == 0 {
			t.Errorf("vendored asset %s is empty", embedded)
		}
		if name == "pbs-plus-codemirror.license.txt" {
			continue
		}
		if !bytes.Contains(source, []byte("/js/"+name)) {
			t.Errorf("pre JS never loads vendored asset %s", name)
		}
	}
}

func TestTargetViewRendersImplementedKindTabs(t *testing.T) {
	source := ui.Render()
	if got := bytes.Count(source, []byte(`xtype: "pbsDiskTargetPanel"`)); got != 6 {
		t.Fatalf("rendered %d target panels, want 6", got)
	}

	for _, expected := range [][]byte{
		[]byte(`path: "pbsD2DTargets"`),
		[]byte(`itemId: "filesystem-targets"`),
		[]byte(`targetKind: "filesystem"`),
		[]byte(`itemId: "s3-targets"`),
		[]byte(`targetKind: "s3"`),
		[]byte(`itemId: "postgresql-targets"`),
		[]byte(`targetKind: "postgresql"`),
		[]byte(`itemId: "mysql-targets"`),
		[]byte(`targetKind: "mysql"`),
		[]byte(`itemId: "ldap-targets"`),
		[]byte(`targetKind: "ldap"`),
		[]byte(`itemId: "dovecot-targets"`),
		[]byte(`targetKind: "dovecot"`),
		[]byte(`subject: "LDAP / Active Directory Target"`),
		[]byte(`subject: "Dovecot Target"`),
		[]byte(`dovecot: "Add Dovecot Target"`),
		[]byte(`ldap: "Add LDAP / Active Directory Target"`),
		[]byte(`name: "ldap_base_dn"`),
		[]byte(`Logical directory-data backup only.`),
		[]byte(`column.setText("S3 URL")`),
		[]byte(`target/tree?kind=" + encodeURIComponent(view.targetKind || "")`),
		[]byte(`target-status?kind=" + encodeURIComponent(view.targetKind || "")`),
		[]byte(`PBS.PlusUtils.API2Request({`),
		[]byte(`resp.result && resp.result.data`),
		[]byte(`connection_status: node.connection_status ?? null`),
		[]byte("this.loadData();\n    if (!this.statusTask) {"),
		[]byte(`view.setLoading(true);`),
		[]byte(`fa fa-spinner fa-pulse`),
		[]byte(`st.volume_total_bytes > 0 ? Proxmox.Utils.format_size(st.volume_total_bytes) : ""`),
		[]byte(`node.set("volume_used", st.volume_used_bytes > 0 ? Proxmox.Utils.format_size(st.volume_used_bytes) : "")`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("target view is missing %q", expected)
		}
	}
	if bytes.Contains(source, []byte("filterTargetKind")) {
		t.Error("target view still filters the complete target tree in the browser")
	}
	for _, stale := range [][]byte{
		[]byte(`if (view.targetKind === "filesystem")`),
		[]byte(`node.target_type === "agent" ? null : node.connection_status`),
		[]byte(`fa fa-cog"></i> Configured`),
		[]byte(`Status unavailable`),
		[]byte(`node.set("connection_status", "error")`),
	} {
		if bytes.Contains(source, stale) {
			t.Errorf("target status view still contains %q", stale)
		}
	}
}

func TestCustomNavigationGroupsPBSPlusViews(t *testing.T) {
	source := ui.Render()
	configurationStart := bytes.Index(source, []byte(`Ext.define("PBS.PlusConfiguration",`))
	managementStart := bytes.Index(source, []byte(`Ext.define("PBS.D2DManagement",`))
	targetsStart := bytes.Index(source, []byte(`Ext.define("PBS.D2DTargets",`))
	snapshotsStart := bytes.Index(source, []byte(`Ext.define("PBS.D2DSnapshotMount",`))
	mountsStart := bytes.Index(source, []byte(`Ext.define("PBS.D2DMounts",`))
	verificationStart := bytes.Index(source, []byte(`Ext.define("PBS.D2DDataVerification",`))
	if configurationStart < 0 || managementStart < 0 || targetsStart < 0 || snapshotsStart < 0 || mountsStart < 0 || verificationStart < 0 {
		t.Fatal("rendered UI is missing a core PBS Plus view")
	}

	configuration := source[configurationStart:managementStart]
	for _, xtype := range [][]byte{
		[]byte(`xtype: "pbsDiskExclusionPanel"`),
		[]byte(`xtype: "pbsDiskScriptPanel"`),
		[]byte(`xtype: "pbsNotificationBatchView"`),
		[]byte(`xtype: "pbsD2DAlertSettings"`),
	} {
		if !bytes.Contains(configuration, xtype) {
			t.Errorf("PBS Plus configuration is missing %q", xtype)
		}
		if bytes.Contains(source[managementStart:targetsStart], xtype) {
			t.Errorf("Backup / Restore still contains %q", xtype)
		}
	}
	if bytes.Contains(source[managementStart:targetsStart], []byte(`xtype: "pbsDiskTokenPanel"`)) {
		t.Error("agent bootstrap remains under Backup / Restore")
	}
	if !bytes.Contains(source[targetsStart:snapshotsStart], []byte(`xtype: "pbsDiskTokenPanel"`)) {
		t.Error("agent bootstrap is missing from Targets")
	}
	if !bytes.Contains(source[snapshotsStart:mountsStart], []byte(`xtype: "pbsPlusSnapshotMountDatastorePanel"`)) {
		t.Error("Snapshots is missing datastore browsers")
	}
	for _, xtype := range [][]byte{[]byte(`xtype: "pbsPlusActiveMountsPanel"`), []byte(`xtype: "pbsPlusMountProfilesPanel"`)} {
		if bytes.Contains(source[snapshotsStart:mountsStart], xtype) {
			t.Errorf("Snapshots still contains %q", xtype)
		}
		if !bytes.Contains(source[mountsStart:verificationStart], xtype) {
			t.Errorf("Mounts is missing %q", xtype)
		}
	}
	if bytes.Contains(source[snapshotsStart:verificationStart], []byte(`xtype: "pbsPlusOutpostsPanel"`)) {
		t.Error("Snapshots or Mounts still contains Outposts")
	}
	if got := bytes.Count(source, []byte("root.insertChild(")); got != 1 {
		t.Fatalf("rendered %d top-level PBS Plus navigation insertions, want 1", got)
	}
	for _, expected := range [][]byte{
		[]byte(`text: "PBS Plus"`),
		[]byte(`title: gettext("PBS Plus Configuration")`),
		[]byte(`title: gettext("Backup / Restore")`),
		[]byte(`title: gettext("Targets")`),
		[]byte(`title: gettext("Snapshots")`),
		[]byte(`title: gettext("Mounts")`),
		[]byte(`title: gettext("Outposts")`),
		[]byte(`title: gettext("Data Verification")`),
		[]byte(`title: gettext("MTF Migration")`),
		[]byte(`id: "pbs_plus"`),
		[]byte(`path: "pbsPlusConfiguration"`),
		[]byte(`id: "backup_targets"`),
		[]byte(`id: "d2d_targets"`),
		[]byte(`id: "snapshot_mount"`),
		[]byte(`id: "d2d_mounts"`),
		[]byte(`path: "pbsD2DMounts"`),
		[]byte(`id: "d2d_outposts"`),
		[]byte(`path: "pbsPlusOutpostsPanel"`),
		[]byte(`id: "d2d_s3_repositories"`),
		[]byte(`path: "pbsPlusS3RepositoriesPanel"`),
		[]byte(`id: "data_verification"`),
		[]byte(`id: "mtf_tapes"`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("PBS Plus navigation is missing %q", expected)
		}
	}
}

func TestTargetEditWindowsRenderCompleteSchemas(t *testing.T) {
	source := ui.Render()

	for _, expected := range [][]byte{
		[]byte(`subject: "Filesystem Target"`),
		[]byte(`subject: "S3 Target"`),
		[]byte(`subject: "PostgreSQL Target"`),
		[]byte(`subject: "MySQL / MariaDB Target"`),
		[]byte(`subject: "LDAP / Active Directory Target"`),
		[]byte(`subject: "Dovecot Target"`),
		[]byte(`name: "s3_endpoint"`),
		[]byte(`name: "s3_bucket"`),
		[]byte(`name: "s3_region"`),
		[]byte(`name: "s3_access_key"`),
		[]byte(`name: "s3_secret_key"`),
		[]byte(`name: "s3_use_ssl"`),
		[]byte(`name: "s3_path_style"`),
		[]byte(`title: gettext("Connection")`),
		[]byte(`title: gettext("TLS")`),
		[]byte(`title: gettext("Client Tools")`),
		[]byte(`name: "database_default_client_dir"`),
		[]byte(`engine: "{targetKind}"`),
		[]byte(`name: "database_password"`),
		[]byte(`name: "database_scope"`),
		[]byte(`name: "destination_database"`),
		[]byte(`mysql: "Add MySQL / MariaDB Target"`),
		[]byte(`ldap: "Add LDAP / Active Directory Target"`),
		[]byte(`dovecot: "Add Dovecot Target"`),
		[]byte(`name: "dovecot_username"`),
		[]byte(`name: "dovecot_mailbox"`),
		[]byte(`name: "dovecot_source_username"`),
		[]byte(`name: "dovecot_destination_username"`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("target editor is missing %q", expected)
		}
	}
	for _, invalid := range [][]byte{[]byte(`engine: "postgresql"`), []byte(`engine: "mysql"`)} {
		if bytes.Contains(source, invalid) {
			t.Errorf("target editor contains invalid cbind template %q", invalid)
		}
	}
}

func TestDatabaseClientSelectorWaitsForStoreCreation(t *testing.T) {
	source := ui.Render()
	if !bytes.Contains(source, []byte(`typeof store.clearFilter !== "function"`)) {
		t.Fatal("database client selector does not guard its uninitialized store")
	}
}

func TestDatabaseJobWindowsHideFilesystemRestorePath(t *testing.T) {
	source := ui.Render()
	for _, expected := range [][]byte{
		[]byte(`subject: "Backup Job"`),
		[]byte(`subject: "Restore Job"`),
		[]byte(`text: "Backup / Restore"`),
		[]byte(`sourcePath.setHidden(structured);`),
		[]byte(`sourcePath.setDisabled(structured);`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("database job editor is missing %q", expected)
		}
	}
}

func TestDatastoreFieldsUseTheSharedSelector(t *testing.T) {
	source := ui.Render()
	if bytes.Contains(source, []byte(`store: "pbs-datastore-list"`)) {
		t.Error("a field still binds to the datastore store id, which is empty when the dialog opens first")
	}
	if bytes.Count(source, []byte(`xtype: "pbsDataStoreSelector"`)) < 3 {
		t.Error("mount dialogs no longer use the shared datastore selector")
	}
}

func TestDatabaseJobWindowsDoNotReferenceRemovedClientOverrides(t *testing.T) {
	source := ui.Render()
	for _, stale := range [][]byte{
		[]byte(`lookup("databaseClient")`),
		[]byte(`lookup("databaseClientFamily")`),
	} {
		if bytes.Contains(source, stale) {
			t.Errorf("database job editor still references removed client override %q", stale)
		}
	}
}

func TestAsyncCallbacksDoNotDereferenceDestroyedViews(t *testing.T) {
	source := ui.Render()
	for _, stale := range [][]byte{
		[]byte(`const bar = me.getView().down("[reference=aggregateBar]");`),
		[]byte(`me.getView().getStore().load();`),
		[]byte(`taskDone: () => me.getView().getStore().load(),`),
	} {
		if bytes.Contains(source, stale) {
			t.Errorf("async callback dereferences a potentially destroyed view: %q", stale)
		}
	}
}

func TestSnapshotSelectorDelegatesCompatibilityToServer(t *testing.T) {
	source := append(compileJS(&preJsFS), ui.Render()...)
	for _, expected := range [][]byte{
		[]byte(`url: pbsPlusBaseUrl + "/api2/json/d2d/restore/snapshots"`),
		[]byte(`target: me.getRestoreTarget() || ""`),
		[]byte(`this.lookup("snapshot").setRestoreTarget(value || null);`),
		[]byte(`snapRecord.get("archive")`),
		[]byte(`snapshot.get("source_target")`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("snapshot selector is missing %q", expected)
		}
	}
	for _, stale := range [][]byte{
		[]byte(`archiveFilter`),
		[]byte(`snapshotHasArchive`),
		[]byte(`archiveName`),
	} {
		if bytes.Contains(source, stale) {
			t.Errorf("frontend still contains archive matching logic %q", stale)
		}
	}
	for _, config := range []string{"RestoreTarget", "PreferredGroup", "Datastore", "Namespace"} {
		if bytes.Contains(source, []byte("apply"+config+": function")) {
			t.Errorf("snapshot selector shadows the ExtJS %s config applier", config)
		}
	}
}

func TestSnapshotSelectorOpensFilterableBrowser(t *testing.T) {
	source := ui.Render()
	for _, expected := range [][]byte{
		[]byte(`Ext.define("PBS.window.D2DSnapshotBrowser", {`),
		[]byte(`Ext.create("PBS.window.D2DSnapshotBrowser", { snapshotField: me, initialHost: me.getPreferredGroup() || "" }).show();`),
		[]byte(`reference: "targetOnly"`),
		[]byte(`iconCls: "fa fa-history", margin: "0 0 0 5", tooltip: gettext("Browse snapshots by host and date")`),
		[]byte(`reference: "hostFilter"`),
		[]byte(`reference: "fromDate"`),
		[]byte(`reference: "toDate"`),
		[]byte(`reference: "latestOnly"`),
		[]byte(`view.getSnapshotField().setValue(rec.get("value"));`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("snapshot browser is missing %q", expected)
		}
	}
}

func TestBackupJobsOfferRestoreShortcut(t *testing.T) {
	source := ui.Render()
	for _, expected := range [][]byte{
		[]byte(`restoreFromJob: function ()`),
		[]byte(`snapshotGroup: job.getId(),`),
		[]byte(`me.lookup("snapshot").setPreferredGroup(me.snapshotGroup);`),
		[]byte(`itemcontextmenu: "onItemContextMenu"`),
		[]byte(`this.selectOriginalTarget(field, value);`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("restore shortcut is missing %q", expected)
		}
	}
}

func TestBackupBulkEditUsesAtomicEndpoint(t *testing.T) {
	source := ui.Render()
	for _, expected := range [][]byte{
		[]byte(`Ext.define("PBS.window.D2DBulkJobEdit", {`),
		[]byte(`url: "/api2/extjs/config/disk-backup-bulk",`),
		[]byte(`method: "PUT",`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("bulk edit is missing %q", expected)
		}
	}
}
