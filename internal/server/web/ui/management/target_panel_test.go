package management

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/web/js"
)

func TestTargetSearchSurvivesReload(t *testing.T) {
	t.Parallel()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "target_controller.js")
	if err := os.WriteFile(path, js.Render(targetPanelController), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"local", "agent", "s3", "postgresql", "mysql", "ldap", "dovecot"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			output, err := exec.Command(node, "testdata/target_search.cjs", path, kind).CombinedOutput()
			if err != nil {
				t.Fatalf("target search reset after reload: %v\n%s", err, output)
			}
		})
	}
}
