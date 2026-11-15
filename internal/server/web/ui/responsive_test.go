package ui

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestResponsiveRuntime(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	output, err := exec.Command(node, "testdata/responsive.cjs", "../views/pre/0_responsive.js").CombinedOutput()
	if err != nil {
		t.Fatalf("responsive runtime failed: %v\n%s", err, output)
	}
}

func TestResponsiveWindowCoverage(t *testing.T) {
	t.Parallel()
	source := string(Render())
	for _, forbidden := range []string{
		`Ext.create("Ext.window.Window",`,
		`extend: "Ext.window.Window",`,
		`height: "15px",`,
	} {
		if strings.Contains(source, forbidden) {
			t.Errorf("UI bypasses responsive layout: %s", forbidden)
		}
	}
	for _, name := range []string{"2_utils.js", "4_task_viewer.js"} {
		data, err := os.ReadFile(filepath.Join("..", "views", "pre", name))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "pbsPlusResponsive") && !strings.Contains(string(data), "PBS.plusWindow.Window") {
			t.Errorf("%s does not use responsive window handling", name)
		}
	}
}
