//go:build linux

package snapshotmount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pxar/backupproxy"
	"github.com/pbs-plus/pxar/buzhash"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/format"
	"github.com/pbs-plus/pxar/transfer"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func runCompose(ctx context.Context, in jobs.SnapshotComposeInput) error {
	key := Key(in.Datastore, in.TargetNS, in.TargetType, in.TargetID, "compose")

	task, err := openTask(in.UPID, "compose", tasklog.FormatWorkerID(in.Datastore, "compose-", key))
	if err != nil {
		return jobs.NonRetryable(err)
	}

	runErr := composeSnapshot(ctx, task, in)
	if runErr != nil {
		task.CloseErr(runErr)
		if errors.Is(runErr, context.Canceled) {
			return runErr
		}
		return jobs.NonRetryable(runErr)
	}
	task.CloseOK()
	return nil
}

func composeSnapshot(ctx context.Context, task *tasklog.WorkerTask, in jobs.SnapshotComposeInput) error {
	if err := validate.ValidateDatastore(in.Datastore); err != nil {
		return fmt.Errorf("invalid datastore: %w", err)
	}
	if err := validate.ValidateNamespace(in.SourceNS); err != nil {
		return err
	}
	if err := validate.ValidateNamespace(in.TargetNS); err != nil {
		return err
	}
	if err := validate.ValidateBackupType(in.SourceType); err != nil {
		return err
	}
	if err := validate.ValidateBackupType(in.TargetType); err != nil {
		return err
	}
	if err := validate.ValidateBackupID(in.SourceID); err != nil {
		return err
	}
	if err := validate.ValidateBackupID(in.TargetID); err != nil {
		return err
	}
	srcTime, err := time.Parse(time.RFC3339, in.SourceTime)
	if err != nil {
		return fmt.Errorf("invalid backup-time format: %w", err)
	}
	if !strings.HasSuffix(in.SourceFile, ".mpxar.didx") && !strings.HasSuffix(in.SourceFile, ".pxar.didx") {
		return fmt.Errorf("source archive must be a .mpxar.didx or .pxar.didx file")
	}
	if len(in.Paths) == 0 {
		return errors.New("no paths selected")
	}
	if in.StripRoot && len(in.Paths) != 1 {
		return errors.New("directory flatten requires exactly one selected directory")
	}

	task.LogString(fmt.Sprintf("composing %s/%s/%s (%s) into %s/%s/%s",
		in.SourceNS, in.SourceType, in.SourceID, in.SourceFile,
		in.TargetNS, in.TargetType, in.TargetID))

	dsInfo, err := cli.GetDatastoreInfo(in.Datastore)
	if err != nil {
		return err
	}
	if dsInfo.Path == "" {
		return fmt.Errorf("datastore %s has no path", in.Datastore)
	}

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path, in.SourceNS, in.SourceType, in.SourceID, DirTime(srcTime.UTC()), in.SourceFile)
	if err != nil {
		return fmt.Errorf("resolve source archive: %w", err)
	}

	chunkStore, err := datastore.NewChunkStore(dsInfo.Path)
	if err != nil {
		return fmt.Errorf("open chunk store: %w", err)
	}
	chunkSource := datastore.NewChunkStoreSource(chunkStore)

	var src transfer.ArchiveReader
	if isSplit {
		src, err = transfer.OpenSplitReader(mpxarPath, ppxarPath, chunkSource)
	} else {
		src, err = transfer.OpenChunkedReader(mpxarPath, chunkSource)
	}
	if err != nil {
		return fmt.Errorf("open source archive: %w", err)
	}
	defer func() {
		_ = src.Close()
	}()

	rootEntry, err := src.ReadRoot()
	if err != nil {
		return fmt.Errorf("read source root: %w", err)
	}
	if in.StripRoot {
		entry, err := src.Lookup(in.Paths[0])
		if err != nil {
			return fmt.Errorf("lookup %q in source: %w", in.Paths[0], err)
		}
		if !entry.IsDir() {
			return fmt.Errorf("%q is not a directory; directory flatten requires one directory", in.Paths[0])
		}
	}

	targetDir := groupParentDir(dsInfo.Path, in.TargetNS, in.TargetType, in.TargetID)
	if err := proxmox.EnsureGroupPath(dsInfo.Path, in.TargetNS, in.TargetType, ""); err != nil {
		return fmt.Errorf("ensure target group dir: %w", err)
	}
	backupTime := uniqueSnapshotTime(targetDir)

	bt, err := datastore.ParseBackupType(in.TargetType)
	if err != nil {
		return fmt.Errorf("invalid backup type %q: %w", in.TargetType, err)
	}

	task.LogString("starting PBS session")
	store := backupproxy.NewPBSStore(backupproxy.PBSConfig{
		BaseURL:       token.DefaultAPIURL,
		Datastore:     in.Datastore,
		AuthToken:     token.ReadLocal(),
		Namespace:     in.TargetNS,
		SkipTLSVerify: true,
	}, func() buzhash.Config {
		cfg, cfgErr := buzhash.NewConfig(4 << 20)
		if cfgErr != nil {
			log.Error(cfgErr, "")
		}
		return cfg
	}(), false)

	lastUploadLog := time.Now()
	lastUploadedBytes := uint64(0)
	var uploadProgress backupproxy.UploadProgress
	onUploadProgress := func(progress backupproxy.UploadProgress) {
		uploadProgress = progress
		now := time.Now()
		elapsed := now.Sub(lastUploadLog)
		if elapsed < 5*time.Second {
			return
		}
		rate := float64(progress.UploadedBytes-lastUploadedBytes) / elapsed.Seconds() / (1 << 20)
		task.LogString(fmt.Sprintf(
			"payload progress: processed %.1f GiB in %d chunks; uploaded %.1f GiB in %d chunks (%.1f MiB/s)",
			float64(progress.ProcessedBytes)/(1<<30), progress.ProcessedChunks,
			float64(progress.UploadedBytes)/(1<<30), progress.UploadedChunks, rate))
		lastUploadLog = now
		lastUploadedBytes = progress.UploadedBytes
	}

	session, err := store.StartSession(ctx, backupproxy.BackupConfig{
		BackupType:       bt,
		BackupID:         in.TargetID,
		BackupTime:       backupTime,
		Namespace:        in.TargetNS,
		PreviousBackup:   previousComposeRef(dsInfo.Path, in, bt, backupTime),
		CryptMode:        datastore.CryptModeNone,
		OnUploadProgress: onUploadProgress,
	})
	if err != nil {
		return fmt.Errorf("start PBS session: %w", err)
	}
	defer func() {
		_ = session.Close()
	}()

	writer, err := transfer.NewRemoteDedupWriter(ctx, session, in.TargetID+".mpxar.didx", in.TargetID+".ppxar.didx")
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}

	if err := writer.Begin(&rootEntry.Metadata, transfer.Options{Format: format.FormatVersion2}); err != nil {
		_ = writer.Close()
		return fmt.Errorf("begin archive: %w", err)
	}

	mappings := make([]transfer.PathMapping, 0, len(in.Paths))
	for _, p := range in.Paths {
		dst := p
		if in.StripRoot {
			dst = "/"
		}
		mappings = append(mappings, transfer.PathMapping{Src: p, Dst: dst})
	}

	task.LogString(fmt.Sprintf("copying %d selected path(s)", len(mappings)))
	copied := 0
	opts := transfer.CopyOption{
		OnProgress: func(_ string, _ uint64) {
			copied++
			if copied%500 == 0 {
				task.LogString(fmt.Sprintf("copied %d entries", copied))
			}
		},
	}
	if err := transfer.Copy(src, writer, mappings, opts); err != nil {
		_ = writer.Close()
		return fmt.Errorf("copy selection: %w", err)
	}
	task.LogString(fmt.Sprintf("copy complete (%d entries); flushing payload uploads", copied))
	if err := writer.Finish(); err != nil {
		return fmt.Errorf("finish writer: %w", err)
	}
	task.LogString(fmt.Sprintf(
		"payload upload complete: processed %.1f GiB in %d chunks; uploaded %.1f GiB in %d chunks",
		float64(uploadProgress.ProcessedBytes)/(1<<30), uploadProgress.ProcessedChunks,
		float64(uploadProgress.UploadedBytes)/(1<<30), uploadProgress.UploadedChunks))

	task.LogString("finalizing PBS snapshot")
	if _, err := session.Finish(ctx); err != nil {
		return fmt.Errorf("finish session: %w", err)
	}
	task.LogString(fmt.Sprintf("composed %s/%s/%s snapshot %s (%d entries)",
		in.TargetNS, in.TargetType, in.TargetID, DirTime(time.Unix(backupTime, 0).UTC()), copied))
	return nil
}

func uniqueSnapshotTime(groupDir string) int64 {
	candidate := time.Now().UTC().Unix()
	for {
		dirName := DirTime(time.Unix(candidate, 0).UTC())
		if _, err := os.Stat(filepath.Join(groupDir, dirName)); err != nil {
			return candidate
		}
		candidate++
	}
}

func previousComposeRef(storeRoot string, in jobs.SnapshotComposeInput, bt datastore.BackupType, backupTime int64) *backupproxy.PreviousBackupRef {
	when, _, err := LatestSnapshotIn(storeRoot, in.TargetNS, in.TargetType, in.TargetID)
	if err != nil {
		return nil
	}
	t, err := time.Parse(time.RFC3339, when)
	if err != nil || t.Unix() >= backupTime {
		return nil
	}
	return &backupproxy.PreviousBackupRef{
		BackupType: bt,
		BackupID:   in.TargetID,
		BackupTime: t.Unix(),
		Namespace:  in.TargetNS,
	}
}
