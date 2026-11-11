package management

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func TestScriptRemovalWarnsAboutUsage(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "script_panel.js")
	if err := os.WriteFile(path, js.Render(scriptPanel), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(node, "testdata/script_remove.cjs", path).CombinedOutput()
	if err != nil {
		t.Fatalf("script removal prompt regressed: %v\n%s", err, output)
	}
}
