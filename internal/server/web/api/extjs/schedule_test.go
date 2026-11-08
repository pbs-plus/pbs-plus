package extjs

import (
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

func TestMissedRuns(t *testing.T) {
	now := time.Now()
	daysAgo := func(d int) int64 { return now.Add(-time.Duration(d) * 24 * time.Hour).Unix() }
	cases := []struct {
		name     string
		schedule string
		history  coredb.JobHistory
		want     int
	}{
		{"unscheduled", "", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(10)}, 0},
		{"never succeeded", "daily", coredb.JobHistory{}, 0},
		{"recent success", "daily", coredb.JobHistory{LastSuccessfulEndtime: now.Add(-time.Minute).Unix(), LastRunUpid: "u", LastRunEndtime: now.Unix()}, 0},
		{"missed three days", "daily", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(3) - 60, LastRunUpid: "u", LastRunEndtime: now.Unix()}, 3},
		{"running", "daily", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(3), LastRunUpid: "u"}, 0},
		{"queued", "daily", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(3), LastRunUpid: "u", LastRunEndtime: now.Unix(), LastRunState: "QUEUED: waiting"}, 0},
		{"capped", "minutely", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(1), LastRunUpid: "u", LastRunEndtime: now.Unix()}, MissedRunLimit},
		{"invalid schedule", "not a schedule", coredb.JobHistory{LastSuccessfulEndtime: daysAgo(3), LastRunUpid: "u", LastRunEndtime: now.Unix()}, 0},
	}
	for _, c := range cases {
		got := MissedRuns(c.schedule, c.history, now)
		if c.name == "missed three days" && (got < 2 || got > 3) {
			t.Errorf("%s: got %d, want 2..3", c.name, got)
			continue
		}
		if c.name != "missed three days" && got != c.want {
			t.Errorf("%s: got %d, want %d", c.name, got, c.want)
		}
	}
}
