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

	// Always compute from now so NextRun reflects the next future occurrence,
	// not a past missed run.
	nextRun, err := calendarevent.ComputeNextEvent(ev, time.Now(), time.Local)
	if err != nil {
		return nil, err
	}

	return &nextRun, nil
}
