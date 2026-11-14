package mount

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func TestProfileLayoutFormAndPreview(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	file := filepath.Join(t.TempDir(), "profiles.js")
	if err := os.WriteFile(file, js.Render(mountProfilesPanel), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := exec.Command(node, "testdata/profile_layout.cjs", file).CombinedOutput()
	if err != nil {
		t.Fatalf("profile layout UI: %v\n%s", err, out)
	}
}
