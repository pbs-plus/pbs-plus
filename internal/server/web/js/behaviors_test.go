package js

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCodeMirrorField(t *testing.T) {
	t.Parallel()
	field := CodeMirrorField("script", "shell", 400)
	if field.XType != XTextArea || field.Name != "script" {
		t.Fatalf("code editor must submit and track changes as a textarea: %#v", field)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	path := filepath.Join(t.TempDir(), "editor.js")
	if err := os.WriteFile(path, []byte(field.AfterRender), 0o600); err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(node, "testdata/code_editor.cjs", path, "../views/pre/2_utils.js").CombinedOutput()
	if err != nil {
		t.Fatalf("code editor runtime failed: %v\n%s", err, output)
	}
}
