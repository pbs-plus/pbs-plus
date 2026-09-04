//go:build linux

package snapshotmount

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/calendar"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

const followLatestInterval = 5 * time.Minute

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

func groupKeyOf(namespace, backupType, backupID string) string {
	return namespace + "\x00" + backupType + "\x00" + backupID
}

// profileShare resolves the share name a profile's sessions attach through.
func profileShare(p Profile) string {
	if p.ShareName != "" {
		return p.ShareName
	}
	return p.Outpost
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
	for _, p := range due {
		reconcileProfile(ctx, engine, p)
	}
}

func AutoMountProfiles(ctx context.Context, engine *jobs.Engine) {
	profiles, err := ListProfiles()
	if err != nil {
		log.Error(err, "auto-mount: listing profiles")
		return
	}
	for _, p := range profiles {
		if p.AutoMount {
			reconcileProfile(ctx, engine, p)
		}
	}
}

func submitUnmount(ctx context.Context, engine *jobs.Engine, session Session, reason string) {
	input := jobs.SnapshotUnmountInput{MountPath: session.MountPoint, Force: false}
	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowSnapshotUnmount, session.ServiceKey, reason, "", input, []string{"snapshot-mount:" + session.ServiceKey}, 1, time.Minute)
	if err != nil {
		log.Error(err, "auto-mount: building unmount submit", "mount-point", session.MountPoint)
		return
	}
	if _, _, err := engine.Submit(ctx, request); err != nil {
		log.Error(err, "auto-mount: submitting unmount workflow", "mount-point", session.MountPoint)
	}
}

func submitBatchMount(ctx context.Context, engine *jobs.Engine, p Profile, g NamespaceGroup, sub string) {
	backupTime, fileName, err := LatestSnapshotIn(mustStoreRoot(p), g.Namespace, g.BackupType, g.BackupID)
	if err != nil {
		log.Error(err, "auto-mount: resolving latest snapshot", "namespace", g.Namespace, "group", g.BackupType+"/"+g.BackupID)
		return
	}
	parsed, err := time.Parse(time.RFC3339, backupTime)
	if err != nil {
		log.Error(err, "auto-mount: parsing backup time", "backup-time", backupTime)
		return
	}
	mountPath := ""
	if p.Outpost == "" && p.MountPath != "" {
		mountPath = filepath.Join(p.MountPath, sub)
	}
	key := Key(p.Datastore, g.Namespace, g.BackupType, g.BackupID, parsed.Format("2006-01-02_15-04-05"))
	input := jobs.SnapshotMountInput{
		Datastore:  p.Datastore,
		Namespace:  g.Namespace,
		BackupType: g.BackupType,
		BackupID:   g.BackupID,
		BackupTime: backupTime,
		FileName:   fileName,
		Mode:       p.Mode,
		Outpost:    p.Outpost,
		ShareName:  profileShare(p),
		SubPath:    sub,
		MountPath:  mountPath,
	}
	request, err := jobs.NewWorkflowSubmit(jobs.WorkflowSnapshotMount, key, "auto-mount", "", input, []string{"snapshot-mount:" + key}, 1, time.Minute)
	if err != nil {
		log.Error(err, "auto-mount: building submit", "namespace", g.Namespace, "group", g.BackupType+"/"+g.BackupID)
		return
	}
	if _, _, err := engine.Submit(ctx, request); err != nil {
		log.Error(err, "auto-mount: submitting mount workflow", "namespace", g.Namespace, "group", g.BackupType+"/"+g.BackupID)
	}
}

func mustStoreRoot(p Profile) string {
	dsInfo, err := cli.GetDatastoreInfo(p.Datastore)
	if err != nil {
		return ""
	}
	return dsInfo.Path
}

// batchSessionsOwned filters sessions this profile reconciles: sub-path
// sessions on the same datastore routed through the same target.
func batchSessionsOwned(p Profile, share string) ([]Session, error) {
	sessions, err := ListSessions()
	if err != nil {
		return nil, err
	}
	var localRoot string
	if p.Outpost == "" && p.MountPath != "" {
		localRoot = filepath.Clean(p.MountPath) + string(os.PathSeparator)
	}
	var owned []Session
	for _, s := range sessions {
		if s.Datastore != p.Datastore || s.SubPath == "" {
			continue
		}
		if p.Outpost != "" {
			if s.Outpost != p.Outpost || s.ShareName != share {
				continue
			}
		} else {
			if s.Outpost != "" {
				continue
			}
			if localRoot != "" && !strings.HasPrefix(s.MountPoint, localRoot) {
				continue
			}
		}
		owned = append(owned, s)
	}
	return owned, nil
}

func sessionLive(s Session) bool {
	if s.MountPoint == "" {
		return true
	}
	return IsMounted(s.MountPoint)
}

// reconcileProfile drives one batch profile toward its desired state: every
// group under the parent namespace mounted at its latest snapshot inside the
// shared target, stale or vanished mounts cleaned up.
func reconcileProfile(ctx context.Context, engine *jobs.Engine, p Profile) {
	share := profileShare(p)
	storeRoot := mustStoreRoot(p)
	if storeRoot == "" {
		log.Error(fmt.Errorf("datastore %s has no path", p.Datastore), "auto-mount: batch reconcile")
		return
	}
	groups, err := ListNamespaceGroups(storeRoot, p.Namespace)
	if err != nil {
		log.Error(err, "auto-mount: listing namespaces", "datastore", p.Datastore, "namespace", p.Namespace)
		return
	}
	subs := planBatch(groups, p.Namespace)
	owned, err := batchSessionsOwned(p, share)
	if err != nil {
		log.Error(err, "auto-mount: listing sessions")
		return
	}

	mountedOfGroup := map[string][]Session{}
	for _, s := range owned {
		gk := groupKeyOf(s.Namespace, s.BackupType, s.BackupID)
		if want, ok := subs[gk]; !ok || want != s.SubPath {
			if s.Mode == ModeRW {
				log.Info("auto-mount: skipping stale read-write session, use mount-now to force", "mount-point", s.MountPoint)
				continue
			}
			if sessionLive(s) {
				submitUnmount(ctx, engine, s, "follow-latest")
			}
			continue
		}
		mountedOfGroup[gk] = append(mountedOfGroup[gk], s)
	}

	for _, g := range groups {
		gk := groupKeyOf(g.Namespace, g.BackupType, g.BackupID)
		live := mountedOfGroup[gk]
		if len(live) == 0 {
			submitBatchMount(ctx, engine, p, g, subs[gk])
			continue
		}
		latestStr, _, err := LatestSnapshotIn(storeRoot, g.Namespace, g.BackupType, g.BackupID)
		if err != nil {
			log.Error(err, "auto-mount: resolving latest snapshot", "namespace", g.Namespace, "group", g.BackupType+"/"+g.BackupID)
			continue
		}
		latest, err := time.Parse(time.RFC3339, latestStr)
		if err != nil {
			continue
		}
		var newest Session
		var newestTime time.Time
		rw := false
		for _, s := range live {
			if !sessionLive(s) {
				continue
			}
			if s.Mode == ModeRW {
				rw = true
			}
			t, err := time.Parse(time.RFC3339, s.BackupTime)
			if err != nil {
				continue
			}
			if newestTime.IsZero() || t.After(newestTime) {
				newest, newestTime = s, t
			}
		}
		if rw {
			log.Info("auto-mount: skipping stale read-write session, use mount-now to force", "datastore", p.Datastore, "group", g.BackupType+"/"+g.BackupID, "mount-point", newest.MountPoint)
			continue
		}
		if newestTime.Before(latest) && p.Replace {
			submitUnmount(ctx, engine, newest, "follow-latest")
		}
	}
}

// ReconcileProfileNow runs one reconcile pass for a single profile (mount-now).
func ReconcileProfileNow(ctx context.Context, engine *jobs.Engine, p Profile) {
	reconcileProfile(ctx, engine, p)
}
