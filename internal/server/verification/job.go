//go:build linux

package verification

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/verification"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

var (
	ErrNoSnapshots       = errors.New("no snapshots found for backup job")
	ErrNoFilesToVerify   = errors.New("no files matched filters for verification")
	ErrAgentNotConnected = errors.New("agent not connected for verification")
	ErrNotAgentTarget    = errors.New("verification requires an agent target")
)

// fileEntry represents a file found in the pxar archive.
type fileEntry struct {
	Path         string
	ContentStart uint64
	ContentEnd   uint64
	Size         int64
}

// verifyState holds the archive reader used to extract file content for hashing.
type verifyState struct {
	fs *vfs.LocalFS
}

// Close releases the verifyState resources.
func (vs *verifyState) Close() error {
	if vs.fs != nil {
		return vs.fs.Close()
	}
	return nil
}

// verificationJob holds state for a verification run.
type verificationJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	task          *VerificationTask
	queueTask     *tasks.QueuedTask
	job           database.VerificationJob
	backupJob     database.Backup
	storeInstance *store.Store
	web           bool

	// result counts set by execute, used by onSuccess/onError to close task
	failedFiles  int
	skippedFiles int
	totalFiles   int
}

// NewVerificationJob creates a new verification job.
func NewVerificationJob(
	job database.VerificationJob,
	storeInstance *store.Store,
	web bool,
) (*jobs.Job, error) {
	v := &verificationJob{
		job:           job,
		storeInstance: storeInstance,
		web:           web,
	}

	return &jobs.Job{
		ID:        job.ID,
		PreExec:   v.preExecute,
		Execute:   v.execute,
		OnSuccess: v.onSuccess,
		OnError:   v.onError,
		Cleanup:   v.cleanup,
	}, nil
}

func (v *verificationJob) preExecute(ctx context.Context) error {
	v.mu.RLock()
	job := v.job
	v.mu.RUnlock()

	backup, err := v.storeInstance.Database.GetBackup(job.BackupJobID)
	if err != nil {
		return fmt.Errorf("failed to get backup job %s: %w", job.BackupJobID, err)
	}

	if !backup.Target.IsAgent() {
		return ErrNotAgentTarget
	}

	v.mu.Lock()
	v.backupJob = backup
	v.mu.Unlock()

	// Create queued task for PBS task viewer
	source := "schedule"
	if v.web {
		source = "web UI"
	}
	queueTask, err := tasks.GenerateVerificationQueuedTask(job, v.web)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create queue task, not fatal").Write()
	} else {
		v.mu.Lock()
		v.queueTask = &queueTask
		v.mu.Unlock()

		// Update job status to show queued
		if err := v.updateJobStatus(false, queueTask.Task); err != nil {
			syslog.L.Error(err).WithMessage("failed to set queue task, not fatal").Write()
		}
	}

	// Log start
	syslog.L.Info().
		WithField("jobID", job.ID).
		WithField("source", source).
		WithMessage("verification job starting").
		Write()

	return nil
}

func (v *verificationJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	v.cancel = cancel

	v.mu.RLock()
	job := v.job
	backup := v.backupJob
	v.mu.RUnlock()

	// Create the PBS verification task
	vTask, err := NewVerificationTask(job)
	if err != nil {
		return fmt.Errorf("failed to create verification task: %w", err)
	}
	v.mu.Lock()
	v.task = vTask
	v.mu.Unlock()

	// Close the queue task file (replaced by real task)
	if v.queueTask != nil {
		v.queueTask.Close()
	}

	// Update job status with real task UPID
	if err := v.updateJobStatus(false, vTask.Task); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job with task UPID").Write()
	}

	vTask.WriteString(fmt.Sprintf("starting verification job '%s' for backup job '%s'", job.ID, job.BackupJobID))

	// Select snapshot
	snapshot, err := v.selectSnapshot(ctx, job, backup)
	if err != nil {
		vTask.WriteString(fmt.Sprintf("failed to select snapshot: %v", err))
		return fmt.Errorf("failed to select snapshot: %w", err)
	}
	vTask.WriteString(fmt.Sprintf("selected snapshot: %s", snapshot.Snapshot))

	// Create result record
	result := &database.VerificationResult{
		VerificationJobID: job.ID,
		UPID:              vTask.UPID,
		Snapshot:          snapshot.Snapshot,
		SnapshotTime:      snapshot.BackupTime,
		Status:            "running",
		StartedAt:         time.Now().Unix(),
		Details:           []database.VerificationFileResult{},
	}
	if err := v.storeInstance.Database.CreateVerificationResult(result); err != nil {
		vTask.WriteString(fmt.Sprintf("failed to create verification result: %v", err))
		return fmt.Errorf("failed to create verification result: %w", err)
	}

	// Open the pxar archive for file extraction
	vs, err := v.openArchive(backup, snapshot)
	if err != nil {
		vTask.WriteString(fmt.Sprintf("failed to open archive: %v", err))
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer func() { _ = vs.Close() }()

	// Enumerate files from the pxar archive and sample
	sampledFiles, err := v.sampleFiles(ctx, job, vs, snapshot)
	if err != nil {
		return fmt.Errorf("failed to sample files: %w", err)
	}

	result.TotalFiles = len(sampledFiles)
	vTask.WriteString(fmt.Sprintf("sampled %d files for verification", len(sampledFiles)))

	// Get agent connection
	hostname := backup.Target.GetHostname()

	var agentCaller interface {
		Call(ctx context.Context, method string, payload any, out any) error
	}

	if sess, ok := v.storeInstance.ARPCAgentsManager.GetStreamPipe(hostname); ok {
		agentCaller = sess
	} else if sess, ok := v.storeInstance.ARPCAgentsManager.GetQuicPipe(hostname); ok {
		agentCaller = sess
	} else {
		return ErrAgentNotConnected
	}

	// Verify each file
	for i, file := range sampledFiles {
		select {
		case <-ctx.Done():
			return jobs.ErrCanceled
		default:
		}

		fileResult := v.verifyFile(ctx, agentCaller, vs, file, backup)
		result.Details = append(result.Details, fileResult)

		switch fileResult.Status {
		case "ok":
			result.VerifiedFiles++
		case "failed":
			result.FailedFiles++
			vTask.WriteString(fmt.Sprintf("file verification failed: %s - %s", file.Path, fileResult.Message))
		default:
			result.SkippedFiles++
			vTask.WriteString(fmt.Sprintf("file skipped: %s - %s", file.Path, fileResult.Message))
		}

		if (i+1)%10 == 0 || i+1 == len(sampledFiles) {
			vTask.WriteString(fmt.Sprintf("progress: %d/%d files verified", i+1, len(sampledFiles)))
		}
	}

	// Store counts for onSuccess/onError to use when closing the task
	v.mu.Lock()
	v.totalFiles = result.TotalFiles
	v.failedFiles = result.FailedFiles
	v.skippedFiles = result.SkippedFiles
	v.mu.Unlock()

	result.Status = "completed"
	result.CompletedAt = time.Now().Unix()

	if err := v.storeInstance.Database.UpdateVerificationResult(*result); err != nil {
		syslog.L.Error(err).WithField("jobID", job.ID).WithMessage("failed to update verification result").Write()
	}

	return nil
}

func (v *verificationJob) onError(err error) {
	syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("verification job failed").Write()

	v.mu.RLock()
	t := v.task
	v.mu.RUnlock()

	if t != nil {
		t.WriteString(fmt.Sprintf("verification job error: %v", err))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		t.CloseErr(err)
	}

	if err := v.updateJobHistory(false, 0); err != nil {
		syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history on error").Write()
	}
}

func (v *verificationJob) onSuccess() {
	syslog.L.Info().WithField("jobID", v.job.ID).WithMessage("verification job completed successfully").Write()

	v.mu.RLock()
	t := v.task
	failed := v.failedFiles
	skipped := v.skippedFiles
	total := v.totalFiles
	v.mu.RUnlock()

	if t != nil {
		verified := total - failed - skipped
		t.WriteString("Verification job summary:")
		t.WriteString(fmt.Sprintf("  total files sampled: %d", total))
		t.WriteString(fmt.Sprintf("  verified: %d", verified))
		t.WriteString(fmt.Sprintf("  failed: %d", failed))
		t.WriteString(fmt.Sprintf("  skipped: %d", skipped))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))

		if failed > 0 {
			t.CloseErr(fmt.Errorf("%d of %d files failed verification", failed, total))
			if err := v.updateJobHistory(false, 0); err != nil {
				syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history").Write()
			}
			return
		}

		if skipped > 0 {
			t.CloseWarn(skipped)
			if err := v.updateJobHistory(true, skipped); err != nil {
				syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history").Write()
			}
			return
		}

		t.CloseOK()
	}

	if err := v.updateJobHistory(true, 0); err != nil {
		syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history on success").Write()
	}
}

func (v *verificationJob) cleanup() {
	if v.cancel != nil {
		v.cancel()
	}
	if v.queueTask != nil {
		v.queueTask.Close()
	}
}

// updateJobStatus updates the verification job's last-run UPID in the database
// (for showing "running" / "queued" status in the UI).
func (v *verificationJob) updateJobStatus(succeeded bool, task proxmox.Task) error {
	job, err := v.storeInstance.Database.GetVerificationJob(v.job.ID)
	if err != nil {
		return err
	}
	job.History.LastRunUpid = task.UPID
	job.History.LastRunStarttime = task.StartTime
	if succeeded {
		job.History.LastSuccessfulUpid = task.UPID
	}
	return v.storeInstance.Database.UpdateVerificationJob(nil, job)
}

// updateJobHistory updates the verification job's history fields after completion
// using the standard PBS task system (mirrors backup/restore pattern).
func (v *verificationJob) updateJobHistory(succeeded bool, warningsNum int) error {
	v.mu.RLock()
	vTask := v.task
	v.mu.RUnlock()

	if vTask == nil {
		return nil
	}

	return jobs.UpdateJobHistory(
		v.job.ID,
		0, // currentPID (not used for verification)
		succeeded,
		warningsNum,
		vTask.Task,
		func() (database.JobHistory, int, error) {
			j, err := v.storeInstance.Database.GetVerificationJob(v.job.ID)
			return j.History, 0, err
		},
		func(history database.JobHistory, _ int) error {
			j, err := v.storeInstance.Database.GetVerificationJob(v.job.ID)
			if err != nil {
				return err
			}
			j.History = history
			return v.storeInstance.Database.UpdateVerificationJob(nil, j)
		},
	)
}

// --- Snapshot selection ---

// snapshotInfo represents a resolved snapshot.
type snapshotInfo struct {
	Snapshot   string // "type/id/time"
	BackupType string
	BackupID   string
	BackupTime int64 // unix timestamp
	Files      []string
}

func (v *verificationJob) selectSnapshot(ctx context.Context, job database.VerificationJob, backup database.Backup) (*snapshotInfo, error) {
	snapshots, err := v.listSnapshots(ctx, backup)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNoSnapshots
	}

	cfg := job.SpotConfig

	if cfg.UseLatest {
		sort.Slice(snapshots, func(i, j int) bool {
			return snapshots[i].BackupTime > snapshots[j].BackupTime
		})
		return &snapshots[0], nil
	}

	filtered := snapshots
	if cfg.DateFrom != "" || cfg.DateTo != "" {
		filtered = filterByDateRange(snapshots, cfg.DateFrom, cfg.DateTo)
		if len(filtered) == 0 {
			return nil, ErrNoSnapshots
		}
	}

	idx := rand.Intn(len(filtered))
	return &filtered[idx], nil
}

func (v *verificationJob) listSnapshots(ctx context.Context, backup database.Backup) ([]snapshotInfo, error) {
	backupID := proxmox.NormalizeHostname(backup.Target.GetHostname())
	backupType := "host"

	dsInfo, err := proxmox.GetDatastoreInfo(backup.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore info: %w", err)
	}

	groupDir := buildSnapshotGroupDir(dsInfo.Path, backupType, backupID, backup.Namespace)

	entries, err := os.ReadDir(groupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNoSnapshots
		}
		return nil, fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	var result []snapshotInfo
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		t, err := time.Parse(time.RFC3339, entry.Name())
		if err != nil {
			continue
		}
		unixTime := t.Unix()

		snapFiles, _ := os.ReadDir(filepath.Join(groupDir, entry.Name()))
		var files []string
		for _, f := range snapFiles {
			files = append(files, f.Name())
		}

		result = append(result, snapshotInfo{
			Snapshot:   fmt.Sprintf("%s/%s/%d", backupType, backupID, unixTime),
			BackupType: backupType,
			BackupID:   backupID,
			BackupTime: unixTime,
			Files:      files,
		})
	}

	return result, nil
}

func buildSnapshotGroupDir(pbsStore, backupType, backupID, namespace string) string {
	base := pbsStore
	if namespace != "" {
		parts := strings.SplitSeq(namespace, "/")
		for p := range parts {
			if p != "" {
				base = filepath.Join(base, "ns", p)
			}
		}
	}
	return filepath.Join(base, backupType, backupID)
}

func filterByDateRange(snapshots []snapshotInfo, dateFrom, dateTo string) []snapshotInfo {
	var fromTime, toTime int64
	if dateFrom != "" {
		if t, err := time.Parse(time.RFC3339, dateFrom); err == nil {
			fromTime = t.Unix()
		} else if t, err := time.Parse("2006-01-02", dateFrom); err == nil {
			fromTime = t.Unix()
		}
	}
	if dateTo != "" {
		if t, err := time.Parse(time.RFC3339, dateTo); err == nil {
			toTime = t.Unix()
		} else if t, err := time.Parse("2006-01-02", dateTo); err == nil {
			toTime = t.Unix()
		}
	}

	var filtered []snapshotInfo
	for _, s := range snapshots {
		if fromTime > 0 && s.BackupTime < fromTime {
			continue
		}
		if toTime > 0 && s.BackupTime > toTime {
			continue
		}
		filtered = append(filtered, s)
	}
	return filtered
}

// --- Archive access ---

// openArchive opens the pxar archive for the given snapshot, returning a
// verifyState that can be used for both file enumeration and content extraction.
func (v *verificationJob) openArchive(backup database.Backup, snap *snapshotInfo) (*verifyState, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(backup.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore info: %w", err)
	}

	t := time.Unix(snap.BackupTime, 0).UTC()
	snapshotTime := t.Format(time.RFC3339)

	var fileName string
	for _, f := range snap.Files {
		if strings.HasSuffix(f, ".mpxar.didx") || strings.HasSuffix(f, ".pxar.didx") {
			fileName = f
			break
		}
	}
	if fileName == "" {
		return nil, fmt.Errorf("no pxar archive found in snapshot")
	}

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path,
		backup.Namespace,
		snap.BackupType,
		snap.BackupID,
		snapshotTime,
		fileName,
	)
	if err != nil {
		return nil, err
	}

	chunkStore, err := datastore.NewChunkStore(dsInfo.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk store: %w", err)
	}
	chunkSource := datastore.NewChunkStoreSource(chunkStore)

	var archiveReader transfer.ArchiveReader

	if isSplit {
		metaData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metadata index: %w", err)
		}
		payloadData, err := os.ReadFile(ppxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload index: %w", err)
		}

		splitReader, err := transfer.NewSplitReader(metaData, payloadData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create split reader: %w", err)
		}
		archiveReader = splitReader
	} else {
		idxData, err := os.ReadFile(mpxarPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read index: %w", err)
		}
		reader, err := transfer.NewChunkedReader(idxData, chunkSource)
		if err != nil {
			return nil, fmt.Errorf("failed to create chunked reader: %w", err)
		}
		archiveReader = reader
	}

	fs := vfs.NewLocalFS(archiveReader)

	return &verifyState{fs: fs}, nil
}

// --- File sampling ---

// sampleFiles walks the pxar archive to enumerate files, then returns a random sample.
func (v *verificationJob) sampleFiles(ctx context.Context, job database.VerificationJob, vs *verifyState, snap *snapshotInfo) ([]fileEntry, error) {
	root, err := vs.fs.Root()
	if err != nil {
		return nil, fmt.Errorf("failed to get archive root: %w", err)
	}

	var allFiles []fileEntry
	allFiles, err = v.walkDir(vs.fs, root, "", allFiles, job.SpotConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to walk archive: %w", err)
	}

	if len(allFiles) == 0 {
		return nil, ErrNoFilesToVerify
	}

	sampleCount := job.SpotConfig.SampleCount
	if sampleCount <= 0 {
		sampleCount = 10
	}
	if sampleCount > len(allFiles) {
		sampleCount = len(allFiles)
	}

	rand.Shuffle(len(allFiles), func(i, j int) {
		allFiles[i], allFiles[j] = allFiles[j], allFiles[i]
	})

	return allFiles[:sampleCount], nil
}

// walkDir recursively walks the pxar directory tree collecting regular files.
func (v *verificationJob) walkDir(fs *vfs.LocalFS, entry *pxar.FileInfo, prefix string, files []fileEntry, cfg database.SpotCheckConfig) ([]fileEntry, error) {
	if entry.IsDir() {
		children, err := fs.ReadDir(entry.EntryRangeStart)
		if err != nil {
			return files, nil
		}
		for _, child := range children {
			childPath := prefix + "/" + child.Name()
			files, err = v.walkDir(fs, &child, childPath, files, cfg)
			if err != nil {
				return files, err
			}
		}
	} else if entry.IsFile() && len(entry.ContentRange) == 2 {
		filePath := prefix

		if !v.matchesFilters(filePath, entry, cfg) {
			return files, nil
		}

		files = append(files, fileEntry{
			Path:         filePath,
			ContentStart: entry.ContentRange[0],
			ContentEnd:   entry.ContentRange[1],
			Size:         entry.Size(),
		})
	}

	return files, nil
}

// matchesFilters checks if a file matches the spot check filter criteria.
func (v *verificationJob) matchesFilters(path string, entry *pxar.FileInfo, cfg database.SpotCheckConfig) bool {
	if len(cfg.Filters) == 0 {
		return true
	}

	for _, filter := range cfg.Filters {
		matches := true

		if filter.PathPattern != "" {
			if strings.Contains(filter.PathPattern, "*") {
				if matched, _ := filepath.Match(filter.PathPattern, filepath.Base(path)); !matched {
					matches = false
				}
			} else {
				if !strings.HasPrefix(path, filter.PathPattern) {
					matches = false
				}
			}
		}

		if filter.MinSize > 0 && entry.Size() < filter.MinSize {
			matches = false
		}
		if filter.MaxSize > 0 && entry.Size() > filter.MaxSize {
			matches = false
		}

		if matches {
			return true
		}
	}

	return false
}

// --- File verification ---

// verifyFile verifies a single file by comparing the agent's SHA-256 hash
// of the live file against the hash of the same file extracted from the
// stored pxar archive.
func (v *verificationJob) verifyFile(
	ctx context.Context,
	agentCaller interface {
		Call(ctx context.Context, method string, payload any, out any) error
	},
	vs *verifyState,
	file fileEntry,
	backup database.Backup,
) database.VerificationFileResult {
	result := database.VerificationFileResult{
		Path:   file.Path,
		Size:   file.Size,
		Status: "error",
	}

	// 1. Ask agent to hash the live file
	rootPath := backup.Target.GetAgentHostPath()
	relPath := strings.TrimPrefix(file.Path, "/")
	if backup.Subpath != "" {
		subpath := strings.TrimPrefix(backup.Subpath, "/")
		relPath = filepath.Join(subpath, relPath)
	}
	agentPath := filepath.Join(rootPath, relPath)

	req := verification.VerifyFileReq{
		FilePath: agentPath,
	}

	var agentResp verification.VerifyFileResp
	if err := agentCaller.Call(ctx, "verify_chunk_file", req, &agentResp); err != nil {
		result.Status = "skipped"
		result.Message = fmt.Sprintf("agent call failed: %v", err)
		return result
	}

	if agentResp.Error != "" {
		result.Status = "skipped"
		result.Message = agentResp.Error
		return result
	}

	// 2. Extract file content from the stored archive and hash it
	storedHash, err := extractFileHash(vs, file)
	if err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("failed to extract file from archive: %v", err)
		return result
	}

	// 3. Compare hashes
	if agentResp.SHA256 != storedHash {
		result.Status = "failed"
		result.Message = fmt.Sprintf("SHA-256 mismatch: agent=%x, archive=%x", agentResp.SHA256, storedHash)
		return result
	}

	result.Status = "ok"
	result.Message = "verified"
	return result
}

// extractFileHash reads a file's content from the pxar archive and returns
// its SHA-256 hash.
func extractFileHash(vs *verifyState, file fileEntry) ([32]byte, error) {
	rc, err := vs.fs.ReadContentReader(file.ContentStart, file.ContentEnd)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to open content reader for [%d, %d): %w", file.ContentStart, file.ContentEnd, err)
	}
	defer func() { _ = rc.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return [32]byte{}, fmt.Errorf("failed to read file content: %w", err)
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
