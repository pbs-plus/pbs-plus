//go:build linux

package restore

import (
	"context"
	"fmt"

	"github.com/pbs-plus/pbs-plus/internal/server/dovecot"
)

func (b *restoreJob) dovecotExecute(ctx context.Context) error {
	stagingDir, err := b.restoreArchive(ctx, ".pbs-plus-dovecot-restore-")
	if err != nil {
		return err
	}
	password, err := b.app.CoreDB.GetDatabasePassword(b.job.DestTarget.Name)
	if err != nil {
		return fmt.Errorf("get Dovecot password: %w", err)
	}
	client, err := dovecot.SelectClient(ctx, b.job.DestTarget)
	if err != nil {
		return err
	}
	if err := dovecot.RestoreBackup(ctx, stagingDir, b.job.DestTarget, password, dovecot.RestoreOptions{
		SourceUsername:      b.job.DovecotSourceUsername,
		DestinationUsername: b.job.DovecotDestinationUsername,
		Mailbox:             b.job.DovecotMailbox,
		ReplaceExisting:     b.job.ReplaceExisting,
		LogWriter:           taskLineWriter{task: b.task},
	}, client); err != nil {
		return err
	}
	b.runPostScript()
	return nil
}
