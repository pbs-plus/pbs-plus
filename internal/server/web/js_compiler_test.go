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

func TestCodeMirrorFieldBuffersValuesUntilEditorLoads(t *testing.T) {
	source := ui.Render()
	for _, marker := range []string{
		"let pendingValue = \"\";",
		"editor.setValue(pendingValue);",
		"values.script = editor.getValue();",
		"editor.setValue(values.script);",
	} {
		if !bytes.Contains(source, []byte(marker)) {
			t.Errorf("rendered JS lost the buffered code mirror wiring: %s", marker)
		}
	}
	if bytes.Contains(source, []byte("editor.codeMirror")) {
		t.Error("rendered JS reads editor.codeMirror directly instead of the buffered field contract")
	}
}

func TestTargetViewRendersImplementedKindTabs(t *testing.T) {
	source := ui.Render()
	if got := bytes.Count(source, []byte(`xtype: "pbsDiskTargetPanel"`)); got != 4 {
		t.Fatalf("rendered %d target panels, want 4", got)
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
		[]byte(`column.setText("S3 URL")`),
		[]byte(`target/tree?kind=" + encodeURIComponent(view.targetKind || "")`),
		[]byte(`if (!this.loaded) {`),
		[]byte(`view.setLoading(true);`),
		[]byte(`fa fa-spinner fa-pulse`),
		[]byte(`Status unavailable`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("target view is missing %q", expected)
		}
	}
	if bytes.Contains(source, []byte("filterTargetKind")) {
		t.Error("target view still filters the complete target tree in the browser")
	}
}

func TestTargetEditWindowsRenderCompleteSchemas(t *testing.T) {
	source := ui.Render()

	for _, expected := range [][]byte{
		[]byte(`subject: "Filesystem Target"`),
		[]byte(`subject: "S3 Target"`),
		[]byte(`subject: "PostgreSQL Target"`),
		[]byte(`subject: "MySQL / MariaDB Target"`),
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
		[]byte(`sourcePath.setHidden(database);`),
		[]byte(`sourcePath.setDisabled(database);`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("database job editor is missing %q", expected)
		}
	}
}

func TestSnapshotSelectorFiltersIncompatibleArchives(t *testing.T) {
	source := ui.Render()
	for _, expected := range [][]byte{
		[]byte(`archiveFilter: null`),
		[]byte(`syncArchiveFilter: function ()`),
		[]byte(`updateArchiveFilter: function ()`),
		[]byte(`name + ".pxar.didx"`),
		[]byte(`name + ".mpxar.didx"`),
		[]byte(`return exclude ? !match : match;`),
		[]byte(`mode: database ? "include" : "exclude"`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("snapshot selector is missing %q", expected)
		}
	}
	if bytes.Contains(source, []byte(`applyArchiveFilter`)) {
		t.Error("snapshot selector shadows the ExtJS archiveFilter config applier")
	}
}
