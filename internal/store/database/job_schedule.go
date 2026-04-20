//go:build linux

package database

import (
	"context"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendarevent"
)

func (backup *Backup) getNextSchedule(ctx context.Context) (*time.Time, error) {
	if backup.Schedule == "" {
		return nil, nil
	}

	ev, err := calendarevent.Parse(backup.Schedule)
	if err != nil {
		return nil, err
	}

	var refTime time.Time
	if backup.History.LastRunStarttime > 0 {
		refTime = time.Unix(backup.History.LastRunStarttime, 0)
	} else {
		refTime = time.Now()
	}

	nextRun, err := calendarevent.ComputeNextEvent(ev, refTime, time.Local)
	if err != nil {
		return nil, err
	}

	return &nextRun, nil
}
