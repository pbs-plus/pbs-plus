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
		[]byte(`Ext.define("PBS.form.D2DTokenSelector"`),
		[]byte(`Ext.define("PBS.form.D2DCalendarEvent"`),
		[]byte(`Ext.define("PBS.form.D2DTargetPathSelector"`),
		[]byte(`Ext.define("PBS.form.D2DSnapshotPathSelector"`),
		[]byte(`Ext.define("PBS.D2DManagement.ExclusionEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TokenEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetS3Secret"`),
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

func TestTargetViewRendersImplementedKindTabs(t *testing.T) {
	source := ui.Render()
	if got := bytes.Count(source, []byte(`xtype: "pbsDiskTargetPanel"`)); got != 2 {
		t.Fatalf("rendered %d target panels, want 2", got)
	}

	for _, expected := range [][]byte{
		[]byte(`path: "pbsD2DTargets"`),
		[]byte(`itemId: "filesystem-targets"`),
		[]byte(`targetKind: "filesystem"`),
		[]byte(`itemId: "s3-targets"`),
		[]byte(`targetKind: "s3"`),
		[]byte(`column.setText("S3 URL")`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("target view is missing %q", expected)
		}
	}
}

func TestTargetEditWindowRendersKindSpecificFields(t *testing.T) {
	source := ui.Render()

	for _, expected := range [][]byte{
		[]byte(`name: "kind"`),
		[]byte(`name: "access"`),
		[]byte(`itemId: "filesystemTargetFields"`),
		[]byte(`itemId: "s3TargetFields"`),
		[]byte(`group.setDisabled(!active)`),
	} {
		if !bytes.Contains(source, expected) {
			t.Errorf("target editor is missing %q", expected)
		}
	}
}
