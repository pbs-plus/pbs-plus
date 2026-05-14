package pxarmount

import (
	"bytes"
	"strings"
	"testing"
)

func TestProgressReporter_PhaseTransitions(t *testing.T) {
	var buf bytes.Buffer
	prog := NewProgressReporter(&buf)

	prog.SetPhase(PhasePrepare)
	prog.SetPhase(PhaseWalk)
	prog.SetPhase(PhaseUpload)
	prog.SetPhase(PhaseVerify)
	prog.SetPhase(PhaseFinalize)
	prog.Done("committed ns/123")

	lines := splitProgressLines(buf.String())
	if len(lines) < 5 {
		t.Fatalf("expected at least 5 progress lines, got %d: %v", len(lines), lines)
	}

	// Last line should be OK
	last := lines[len(lines)-1]
	if !strings.HasPrefix(last, "OK ") {
		t.Errorf("last line = %q, want OK prefix", last)
	}

	// All others should be PROGRESS
	for _, l := range lines[:len(lines)-1] {
		if !strings.HasPrefix(l, "PROGRESS ") {
			t.Errorf("progress line = %q, want PROGRESS prefix", l)
		}
	}
}

func TestProgressReporter_FileCounting(t *testing.T) {
	var buf bytes.Buffer
	prog := NewProgressReporter(&buf)

	// Use SetMsg to bypass throttle and trigger sends.
	prog.AddFile(1024)
	prog.SetMsg("after file 1")
	prog.AddFile(2048)
	prog.SetMsg("after file 2")
	prog.AddFile(4096)
	prog.SetMsg("after file 3")

	lines := splitProgressLines(buf.String())

	// Last progress line should have the cumulative file count.
	last := lines[len(lines)-1]
	if !strings.Contains(last, "3 files") {
		t.Errorf("last progress = %q, should contain '3 files'", last)
	}
	if !strings.Contains(last, "7.0 KiB") {
		t.Errorf("last progress = %q, should contain '7.0 KiB'", last)
	}
}

func TestProgressReporter_ErrorMessage(t *testing.T) {
	var buf bytes.Buffer
	prog := NewProgressReporter(&buf)

	prog.SetPhase(PhaseWalk)
	prog.Error("something went wrong")

	lines := splitProgressLines(buf.String())
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %v", len(lines), lines)
	}
	if !strings.HasPrefix(lines[0], "PROGRESS ") {
		t.Errorf("line[0] = %q, want PROGRESS prefix", lines[0])
	}
	if !strings.HasPrefix(lines[1], "ERR ") {
		t.Errorf("line[1] = %q, want ERR prefix", lines[1])
	}
	if !strings.Contains(lines[1], "something went wrong") {
		t.Errorf("line[1] = %q, should contain error message", lines[1])
	}
}

func TestProgressReporter_Throttle(t *testing.T) {
	var buf bytes.Buffer
	prog := NewProgressReporter(&buf)

	// Rapid calls — should throttle (100ms window).
	for i := range 100 {
		prog.AddFile(int64(i * 100))
	}

	lines := splitProgressLines(buf.String())
	// Should have far fewer than 100 lines due to throttling.
	if len(lines) > 20 {
		t.Errorf("expected throttled output (< 20 lines), got %d", len(lines))
	}
}

func TestProgressReporter_CustomMessage(t *testing.T) {
	var buf bytes.Buffer
	prog := NewProgressReporter(&buf)

	prog.SetMsg("Scanning overlay (backed: 42 dirs)")

	lines := splitProgressLines(buf.String())
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "Scanning overlay (backed: 42 dirs)") {
		t.Errorf("line = %q, should contain custom message", lines[0])
	}
}

func TestProgressDisplay_Update(t *testing.T) {
	var buf bytes.Buffer
	display := NewProgressDisplay(&buf)

	display.Update("PROGRESS Scanning files (5 files, 1.2 MiB)")

	output := buf.String()
	if !strings.Contains(output, "Scanning files") {
		t.Errorf("output = %q, should contain message", output)
	}
	if !strings.Contains(output, "\r") {
		t.Errorf("output = %q, should contain carriage return", output)
	}
}

func TestProgressDisplay_Done(t *testing.T) {
	var buf bytes.Buffer
	display := NewProgressDisplay(&buf)

	display.Update("PROGRESS Working")
	buf.Reset()
	display.Done("OK committed ns/backup1")

	output := buf.String()
	if !strings.Contains(output, "✓") {
		t.Errorf("output = %q, should contain checkmark", output)
	}
	if !strings.Contains(output, "committed ns/backup1") {
		t.Errorf("output = %q, should contain commit info", output)
	}
}

func TestProgressDisplay_Error(t *testing.T) {
	var buf bytes.Buffer
	display := NewProgressDisplay(&buf)

	display.Update("PROGRESS Working")
	buf.Reset()
	display.Error("ERR connection refused")

	output := buf.String()
	if !strings.Contains(output, "✗") {
		t.Errorf("output = %q, should contain X mark", output)
	}
	if !strings.Contains(output, "connection refused") {
		t.Errorf("output = %q, should contain error", output)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
		{1572864, "1.5 MiB"},
		{1073741824, "1.0 GiB"},
		{1610612736, "1.5 GiB"},
	}
	for _, tt := range tests {
		got := formatBytes(tt.bytes)
		if got != tt.want {
			t.Errorf("formatBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
		}
	}
}

func TestProgressState_Percent(t *testing.T) {
	s := ProgressState{Files: 5, TotalFiles: 10}
	if p := s.Percent(); p != 50.0 {
		t.Errorf("Percent() = %f, want 50.0", p)
	}

	s = ProgressState{Files: 0, TotalFiles: 0}
	if p := s.Percent(); p != -1 {
		t.Errorf("Percent() = %f, want -1 (unknown)", p)
	}
}

func splitProgressLines(s string) []string {
	var lines []string
	for l := range strings.SplitSeq(s, "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}
