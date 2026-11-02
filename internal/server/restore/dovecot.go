//go:build linux

package restore

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/server/dovecot"
)

func (b *restoreJob) dovecotExecute(ctx context.Context) error {
	sources := []string{"/"}
	if b.job.DovecotSourceUsername != "" {
		sources = dovecot.ArchiveUserPaths(b.job.DovecotSourceUsername)
	}
	stagingDir, err := b.restoreArchiveSources(ctx, ".pbs-plus-dovecot-restore-", sources)
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
		SourceMailbox:       b.job.DovecotMailbox,
		DestinationMailbox:  b.job.DovecotDestinationMailbox,
		Messages:            dovecotMessages(b.job.DovecotMessages),
		ReplaceExisting:     b.job.ReplaceExisting,
		LogWriter:           taskLineWriter{task: b.task},
	}, client); err != nil {
		return err
	}
	b.runPostScript()
	return nil
}

func dovecotMessages(raw string) []string {
	var messages []string
	for line := range strings.SplitSeq(raw, "\n") {
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			messages = append(messages, trimmed)
		}
	}
	return messages
}
