package jobs

import (
	"slices"
	"testing"
)

func TestScriptOutputStreamsCompleteLines(t *testing.T) {
	var lines []string
	output := &scriptOutput{onLine: func(line string) {
		lines = append(lines, line)
	}}

	first := []byte("first\nsecond")
	if n, err := output.Write(first); err != nil || n != len(first) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if !slices.Equal(lines, []string{"first"}) {
		t.Fatalf("lines before completion = %q", lines)
	}

	second := []byte(" part\r\nlast")
	if n, err := output.Write(second); err != nil || n != len(second) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	output.Flush()

	wantLines := []string{"first", "second part", "last"}
	if !slices.Equal(lines, wantLines) {
		t.Fatalf("lines = %q, want %q", lines, wantLines)
	}
	if got, want := output.String(), "first\nsecond part\r\nlast"; got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
}
