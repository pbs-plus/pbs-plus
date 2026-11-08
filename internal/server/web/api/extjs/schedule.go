package extjs

import (
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	missedRunGrace = time.Hour
	MissedRunLimit = 100
)

// MissedRuns counts scheduled occurrences that elapsed, past a grace period, without a successful run.
func MissedRuns(schedule string, history coredb.JobHistory, now time.Time) int {
	if schedule == "" || history.LastSuccessfulEndtime == 0 {
		return 0
	}
	if history.LastRunUpid != "" && (history.LastRunEndtime == 0 || strings.HasPrefix(history.LastRunState, "QUEUED:")) {
		return 0
	}
	ev, err := calendar.Parse(schedule)
	if err != nil {
		return 0
	}
	return calendar.CountEvents(ev, time.Unix(history.LastSuccessfulEndtime, 0), now.Add(-missedRunGrace), time.Local, MissedRunLimit)
}
