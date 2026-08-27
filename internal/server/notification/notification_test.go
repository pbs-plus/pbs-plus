//go:build linux

package notification

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriteSpoolFile_PublishesCompleteJSONOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "n1.json")

	if err := writeSpoolFile(path, []byte(`{"ok":true}`)); err != nil {
		t.Fatalf("writeSpoolFile: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only the published file, got %d entries", len(entries))
	}
	if entries[0].Name() != "n1.json" {
		t.Fatalf("expected n1.json, got %q", entries[0].Name())
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		t.Fatalf("published spool file is not valid JSON: %v", err)
	}
}

func TestWriteSpoolFile_LeavesNoTempOnFailure(t *testing.T) {
	dir := t.TempDir()
	if err := writeSpoolFile(filepath.Join(dir, "missing", "n.json"), []byte("{}")); err == nil {
		t.Fatal("expected an error writing into a nonexistent directory")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Fatalf("leaked temp spool file %q", e.Name())
		}
	}
}

func TestNormalizeSeverity_ClampsUnknownValues(t *testing.T) {
	for _, valid := range []string{"info", "notice", "warning", "error", "unknown"} {
		if got := normalizeSeverity(valid); got != valid {
			t.Errorf("normalizeSeverity(%q) = %q, want unchanged", valid, got)
		}
	}
	if got := normalizeSeverity("critical"); got != "info" {
		t.Errorf("normalizeSeverity(%q) = %q, want %q", "critical", got, "info")
	}
	if got := normalizeSeverity(""); got != "info" {
		t.Errorf("normalizeSeverity(%q) = %q, want %q", "", got, "info")
	}
}
