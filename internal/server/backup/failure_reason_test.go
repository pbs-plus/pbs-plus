//go:build linux

package backup

import (
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func TestFailureReasonFromLineKeepsTheRealPBSError(t *testing.T) {
	cases := []struct {
		name string
		line string
		want string
	}{
		{
			name: "connection error is the phoenix-pbs failure",
			line: "2026-10-31T19:02:32-04:00: backup failed: connection error",
			want: "backup failed: connection error",
		},
		{
			name: "upload failure is kept",
			line: "2026-10-31T19:02:32-04:00: upload failed: broken pipe",
			want: "upload failed: broken pipe",
		},
		{
			name: "task error prefix is stripped",
			line: "2026-10-31T19:02:32-04:00: TASK ERROR: lost connection with backup agent",
			want: "lost connection with backup agent",
		},
		{
			name: "our own placeholder is never echoed back",
			line: "2026-10-31T19:03:38-04:00: TASK ERROR: " + jobs.ErrUnexpected.Error(),
			want: "",
		},
		{
			name: "ordinary progress lines carry no reason",
			line: "2026-10-31T18:07:27-04:00: protocol upgrade done",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := failureReasonFromLine(tc.line); got != tc.want {
				t.Fatalf("failureReasonFromLine(%q) = %q, want %q", tc.line, got, tc.want)
			}
		})
	}
}
