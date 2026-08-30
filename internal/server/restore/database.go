//go:build linux

package restore

import (
	"context"
	"fmt"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
)

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

	bundle, err := database.ResolveClientBundle(ctx, b.job.DestTarget, b.job.DatabaseClientFamily, b.job.DatabaseClientDir)
	if err != nil {
		return err
	}
	password, err := b.app.CoreDB.GetDatabasePassword(b.job.DestTarget.Name)
	if err != nil {
		return fmt.Errorf("get database password: %w", err)
	}
	if err := database.RestoreDump(ctx, stagingDir, b.job.DestTarget, password, database.RestoreOptions{
		DestinationDatabase: b.job.DestinationDatabase,
		ReplaceExisting:     b.job.ReplaceExisting,
	}, bundle); err != nil {
		return err
	}
	b.runPostScript()
	return nil
}
