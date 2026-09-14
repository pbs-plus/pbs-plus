//go:build linux

package plugins

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/targetplugin"
)

func TestValidateBackupPath(t *testing.T) {
	workspace := t.TempDir()
	workspaceSource := filepath.Join(workspace, "source")
	if err := os.Mkdir(workspaceSource, 0o700); err != nil {
		t.Fatal(err)
	}
	fields := []targetplugin.FormField{{Key: "path", Control: targetplugin.ControlPath}}
	if err := validateBackupPath(workspace, workspaceSource, fields, nil); err != nil {
		t.Fatalf("workspace source: %v", err)
	}

	configuredSource := t.TempDir()
	config := targetplugin.Values{"path": targetplugin.NewStringScalar(configuredSource)}
	if err := validateBackupPath(workspace, configuredSource, fields, config); err != nil {
		t.Fatalf("configured source: %v", err)
	}
	outside := t.TempDir()
	if err := validateBackupPath(workspace, outside, fields, nil); err == nil {
		t.Fatal("outside source accepted")
	}

	link := filepath.Join(workspace, "link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	if err := validateBackupPath(workspace, link, fields, nil); err == nil {
		t.Fatal("escaping symlink accepted")
	}
}
