package web

import (
	"bytes"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/web/ui"
)

func TestCustomJSMigratesDefinitionsOnce(t *testing.T) {
	source := append(compileJS(&customJsFS, isMigratedCustomSource), ui.Render()...)

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
		[]byte(`Ext.define("PBS.D2DManagement.TargetPanelController"`),
		[]byte(`Ext.define("PBS.D2DManagement.TargetPanel"`),
		[]byte(`Ext.define("PBS.D2DManagement.Alerts"`),
		[]byte(`Ext.define("PBS.D2DManagement.AlertEditWindow"`),
		[]byte(`PBS.D2DManagement.makeNotificationTab =`),
		[]byte(`Ext.define("PBS.D2DManagement.RestoreJobEdit"`),
		[]byte(`Ext.define("PBS.D2DManagement.BackupJobEdit"`),
		[]byte(`Ext.define("PBS.MtfManagement.JobEdit"`),
		[]byte(`Ext.define("PBS.D2DManagement.NotificationBatchEdit"`),
		[]byte(`Ext.define("PBS.form.D2DScriptSelector"`),
		[]byte(`Ext.define("PBS.D2DManagement.ScriptEditWindow"`),
		[]byte(`Ext.define("PBS.D2DManagement.ScriptPanel"`),
	} {
		if got := bytes.Count(source, name); got != 1 {
			t.Errorf("%s is defined %d times, want 1", name, got)
		}
	}
}
