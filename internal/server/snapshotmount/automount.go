//go:build linux

package snapshotmount

import (
	"context"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

const followLatestInterval = 5 * time.Minute

type remountAction int

const (
	remountNone remountAction = iota
	remountMount
	remountUnmount
	remountSkipRW
)

func groupKeyOf(datastore, ns, backupType, backupID string) string {
	return datastore + "\x00" + ns + "\x00" + backupType + "\x00" + backupID
}

type followGate struct {
	lastRun map[string]time.Time
}

func (g *followGate) due(p Profile, now time.Time) bool {
	if p.Schedule == "" {
		return true
	}
	ev, err := calendar.Parse(p.Schedule)
	if err != nil {
		return true
	}
	ref, ok := g.lastRun[p.ID()]
	if !ok {
		ref = now.Add(-followLatestInterval)
	}
	next, err := calendar.ComputeNextEvent(ev, ref, time.Local)
	if err != nil || next.After(now) {
		return false
	}
	if g.lastRun == nil {
		g.lastRun = make(map[string]time.Time)
	}
	g.lastRun[p.ID()] = next
	return true
}

func decideRemount(p Profile, mountedOfGroup []Session, latest time.Time) (remountAction, Session) {
	for _, s := range mountedOfGroup {
		if s.Mode == ModeRW {
			return remountSkipRW, Session{}
		}
	}
	var newest Session
	var newestTime time.Time
	for _, s := range mountedOfGroup {
		if p.MountPath != "" && s.MountPoint != p.MountPath {
			continue
		}
		if p.MountPath == "" {
			if p.Outpost != "" {
				if s.Outpost != p.Outpost {
					continue
				}
				if p.ShareName != "" && s.ShareName != p.ShareName {
					continue
				}
			} else {
				parsed, err := time.Parse(time.RFC3339, s.BackupTime)
				if err != nil {
					continue
				}
				if s.MountPoint != DefaultMountPoint(s.Datastore, s.Namespace, s.BackupType, s.BackupID, parsed) {
					continue
				}
			}
		}
		t, err := time.Parse(time.RFC3339, s.BackupTime)
		if err != nil {
			continue
		}
		if newestTime.IsZero() || t.After(newestTime) {
			newest, newestTime = s, t
		}
	}
	switch {
	case newestTime.IsZero():
		return remountMount, Session{}
	case newestTime.Before(latest):
		return remountUnmount, newest
	default:
		return remountNone, Session{}
	}
}

func FollowLatestProfiles(ctx context.Context, engine *jobs.Engine) {
	AutoMountProfiles(ctx, engine)
	gate := &followGate{}
	ticker := time.NewTicker(followLatestInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			autoMountProfilesGated(ctx, engine, gate)
		}
	}
}

func autoMountProfilesGated(ctx context.Context, engine *jobs.Engine, gate *followGate) {
	now := time.Now()
	profiles, err := ListProfiles()
	if err != nil {
		log.Error(err, "auto-mount: listing profiles")
		return
	}
	var due []Profile
	for _, p := range profiles {
		if p.AutoMount && gate.due(p, now) {
			due = append(due, p)
		}
	}
	autoMountProfiles(ctx, engine, due)
}

func AutoMountProfiles(ctx context.Context, engine *jobs.Engine) {
	autoMountProfiles(ctx, engine, nil)
}

func autoMountProfiles(ctx context.Context, engine *jobs.Engine, only []Profile) {
	profiles, err := ListProfiles()
	if err != nil {
		log.Error(err, "auto-mount: listing profiles")
		return
	}
	sessions, err := ListSessions()
	if err != nil {
		log.Error(err, "auto-mount: listing sessions")
		return
	}
	mounted := make(map[string][]Session)
	for _, s := range sessions {
		if !IsMounted(s.MountPoint) {
			continue
		}
		key := groupKeyOf(s.Datastore, s.Namespace, s.BackupType, s.BackupID)
		mounted[key] = append(mounted[key], s)
	}
	iterate := profiles
	if only != nil {
		iterate = only
	}
	for _, p := range iterate {
		group := groupKeyOf(p.Datastore, p.Namespace, p.BackupType, p.BackupID)
		backupTime, fileName, err := LatestSnapshot(p)
		if err != nil {
			log.Error(err, "auto-mount: resolving latest snapshot", "datastore", p.Datastore, "group", p.BackupType+"/"+p.BackupID)
			continue
		}
		parsed, err := time.Parse(time.RFC3339, backupTime)
		if err != nil {
			log.Error(err, "auto-mount: parsing backup time", "backup-time", backupTime)
			continue
		}
		action, target := decideRemount(p, mounted[group], parsed)
		switch action {
		case remountSkipRW:
			log.Info("auto-mount: skipping stale read-write session, use mount-now to force", "datastore", p.Datastore, "group", p.BackupType+"/"+p.BackupID, "mount-point", target.MountPoint)
			continue
		case remountNone:
			continue
		case remountUnmount:
			input := jobs.SnapshotUnmountInput{MountPath: target.MountPoint}
			request, err := jobs.NewWorkflowSubmit(jobs.WorkflowSnapshotUnmount, target.ServiceKey, "follow-latest", "", input, []string{"snapshot-mount:" + target.ServiceKey}, 1, time.Minute)
			if err != nil {
				log.Error(err, "auto-mount: building unmount submit", "mount-point", target.MountPoint)
				continue
			}
			if _, _, err := engine.Submit(ctx, request); err != nil {
				log.Error(err, "auto-mount: submitting unmount workflow", "mount-point", target.MountPoint)
			}
			continue
		}
		key := Key(p.Datastore, p.Namespace, p.BackupType, p.BackupID, parsed.Format("2006-01-02_15-04-05"))
		input := jobs.SnapshotMountInput{
			Datastore:  p.Datastore,
			Namespace:  p.Namespace,
			BackupType: p.BackupType,
			BackupID:   p.BackupID,
			BackupTime: backupTime,
			FileName:   fileName,
			Mode:       p.Mode,
			Backend:    p.Backend,
			Outpost:    p.Outpost,
			ShareName:  p.ShareName,
			MountPath:  p.MountPath,
		}
		request, err := jobs.NewWorkflowSubmit(jobs.WorkflowSnapshotMount, key, "auto-mount", "", input, []string{"snapshot-mount:" + key}, 1, time.Minute)
		if err != nil {
			log.Error(err, "auto-mount: building submit", "group", p.BackupType+"/"+p.BackupID)
			continue
		}
		if _, _, err := engine.Submit(ctx, request); err != nil {
			log.Error(err, "auto-mount: submitting mount workflow", "group", p.BackupType+"/"+p.BackupID)
		}
	}
}
