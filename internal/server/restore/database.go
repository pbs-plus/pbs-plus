//go:build linux

package restore

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

type taskLineWriter struct {
	task *RestoreTask
}

func (w taskLineWriter) Write(data []byte) (int, error) {
	text := strings.TrimSuffix(string(data), "\n")
	if text != "" && w.task != nil {
		for line := range strings.SplitSeq(text, "\n") {
			w.task.WriteString(line)
		}
	}
	return len(data), nil
}

func (b *restoreJob) databaseExecute(ctx context.Context) error {
	stagingDir, err := os.MkdirTemp("", ".pbs-plus-database-restore-")
	if err != nil {
		return fmt.Errorf("create database restore staging directory: %w", err)
	}
	if err := os.Chmod(stagingDir, 0o700); err != nil {
		_ = os.RemoveAll(stagingDir)
		return fmt.Errorf("secure database restore staging directory: %w", err)
	}
	b.databaseStagingDir = stagingDir

	if err := b.startLocalRestore(ctx, stagingDir, []string{"/"}, pxar.RestoreModeNormal); err != nil {
		return err
	}
	if err := b.waitForTransfer(ctx); err != nil {
		return err
	}

	password, err := b.app.CoreDB.GetDatabasePassword(b.job.DestTarget.Name)
	if err != nil {
		return fmt.Errorf("get database password: %w", err)
	}
	bundle, err := database.SelectClientBundle(ctx, b.job.DestTarget, password, taskLineWriter{task: b.task})
	if err != nil {
		return err
	}
	if err := database.RestoreDump(ctx, stagingDir, b.job.DestTarget, password, database.RestoreOptions{
		SourceDatabase:      b.job.SourceDatabase,
		DestinationDatabase: b.job.DestinationDatabase,
		ReplaceExisting:     b.job.ReplaceExisting,
	}, bundle); err != nil {
		return err
	}
	b.runPostScript()
	return nil
}
