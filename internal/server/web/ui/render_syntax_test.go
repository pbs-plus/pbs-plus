package ui

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRenderIsValidJavaScript(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}

	path := filepath.Join(t.TempDir(), "pbs-plus.js")
	if err := os.WriteFile(path, Render(), 0o600); err != nil {
		t.Fatal(err)
	}

	output, err := exec.Command(node, "--check", path).CombinedOutput()
	if err != nil {
		t.Fatalf("rendered UI is not valid JavaScript: %v\n%s", err, output)
	}
}
