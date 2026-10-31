package proxmox

import (
	"testing"
	"time"
)

func TestTrimLogLineTimestampRemovesOnlyAFormatLogLineStamp(t *testing.T) {
	stamped := FormatLogLine(time.Date(2026, 10, 31, 19, 3, 36, 0, time.FixedZone("EDT", -4*3600)), "pbs-plus: hello")
	if got := TrimLogLineTimestamp(stamped); got != "pbs-plus: hello" {
		t.Fatalf("got %q, want the message without its stamp", got)
	}

	for _, line := range []string{
		"processed 1.203 TiB in 1h 38m",
		"TASK ERROR: upload failed",
		"no colon space here",
		"",
	} {
		if got := TrimLogLineTimestamp(line); got != line {
			t.Errorf("TrimLogLineTimestamp(%q) = %q, want it unchanged", line, got)
		}
	}
}

func TestTrimLogLineTimestampIsIdempotent(t *testing.T) {
	once := TrimLogLineTimestamp(FormatLogLine(time.Now(), "a message"))
	if twice := TrimLogLineTimestamp(once); twice != once {
		t.Fatalf("second trim changed %q into %q", once, twice)
	}
}
