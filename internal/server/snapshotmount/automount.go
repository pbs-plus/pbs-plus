//go:build linux

package snapshotmount

import (
	"context"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

func AutoMountProfiles(ctx context.Context, engine *jobs.Engine) {
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
	mounted := make(map[string]bool, len(sessions))
	for _, s := range sessions {
		if s.Mode == ModeRO || s.Mode == ModeRW {
			mounted[s.Datastore+"\x00"+s.Namespace+"\x00"+s.BackupType+"\x00"+s.BackupID] = IsMounted(s.MountPoint)
		}
	}
	for _, p := range profiles {
		if !p.AutoMount {
			continue
		}
		group := p.Datastore + "\x00" + p.Namespace + "\x00" + p.BackupType + "\x00" + p.BackupID
		if mounted[group] {
			continue
		}
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
		key := Key(p.Datastore, p.Namespace, p.BackupType, p.BackupID, parsed.Format("2006-01-02_15-04-05"))
		input := jobs.SnapshotMountInput{
			Datastore:  p.Datastore,
			Namespace:  p.Namespace,
			BackupType: p.BackupType,
			BackupID:   p.BackupID,
			BackupTime: backupTime,
			FileName:   fileName,
			Mode:       p.Mode,
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
