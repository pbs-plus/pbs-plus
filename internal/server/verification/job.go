//go:build linux

package verification

import (
	"context"
	"errors"
	"fmt"
	sha256simd "github.com/minio/sha256-simd"
	"io"
	"math"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/verification"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// weightedShuffleBackups reorders backup jobs using weighted random selection
// was last successfully verified. Jobs that have never been verified receive
// the maximum weight. This ensures uniform coverage over successive runs and
// prevents the same backup job from being selected repeatedly.
func weightedShuffleBackups(backups []database.Backup, db *database.Database, verificationJobID string) []database.Backup {
	if len(backups) <= 1 {
		return backups
	}

	lastVerified := make(map[string]int64)
	results, err := db.GetVerificationResults(verificationJobID)
	if err == nil {
		for _, r := range results {
			if r.Status != "completed" {
				continue
			}
			parts := strings.SplitN(r.Snapshot, "/", 3)
			if len(parts) >= 2 {
				hostname := proxmox.NormalizeHostname(parts[1])
				if r.CompletedAt > lastVerified[hostname] {
					lastVerified[hostname] = r.CompletedAt
				}
			}
		}
	}

	now := time.Now().Unix()

	// Compute weights: inverse of seconds since last verification.
	// Never-verified jobs get a very large weight.
	weights := make([]float64, len(backups))
	for i, b := range backups {
		hostname := proxmox.NormalizeHostname(b.Target.GetHostname())
		last := lastVerified[hostname]
		if last == 0 {
			// Never verified  -  maximum weight
			weights[i] = float64(now)
		} else {
			elapsed := float64(now - last)
			if elapsed < 1 {
				elapsed = 1 // minimum 1 second gap to avoid zero
			}
			weights[i] = elapsed
		}
	}

	// Pick a random index proportional to weight, move it to the output,
	remaining := make([]int, len(backups))
	for i := range remaining {
		remaining[i] = i
	}

	buf, cryptoErr := crypto.SecureRandomBytes(len(backups) * 4)
	if cryptoErr != nil {
		// Fallback: just shuffle with math/rand
		mrand.Shuffle(len(backups), func(i, j int) {
			backups[i], backups[j] = backups[j], backups[i]
		})
		return backups
	}

	result := make([]database.Backup, 0, len(backups))
	remWeights := make([]float64, len(weights))
	copy(remWeights, weights)

	for sel := range backups {
		remTotal := 0.0
		for _, idx := range remaining[sel:] {
			remTotal += remWeights[idx]
		}
		if remTotal <= 0 {
			remTotal = 1
		}

		// Pick a random threshold using crypto/rand
		raw := uint32(buf[sel*4]) | uint32(buf[sel*4+1])<<8 | uint32(buf[sel*4+2])<<16 | uint32(buf[sel*4+3])<<24
		threshold := float64(raw%1_000_000) / 1_000_000.0 * remTotal

		cumulative := 0.0
		chosen := remaining[sel]
		for _, idx := range remaining[sel:] {
			cumulative += remWeights[idx]
			if cumulative >= threshold {
				chosen = idx
				break
			}
		}

		for j := sel; j < len(remaining); j++ {
			if remaining[j] == chosen {
				remaining[sel], remaining[j] = remaining[j], remaining[sel]
				break
			}
		}
		result = append(result, backups[chosen])
	}

	return result
}

var (
	ErrNoSnapshots       = errors.New("no snapshots found for backup job")
	ErrNoFilesToVerify   = errors.New("no files matched filters for verification")
	ErrAgentNotConnected = errors.New("agent not connected for verification")
	ErrNotAgentTarget    = errors.New("verification requires an agent target")

	// bufPool provides reusable 256KB buffers for streaming file hashes.
	bufPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 256*1024)
			return &buf
		},
	}

	// activeJobs tracks currently running verification jobs for cancellation.
	activeJobs = struct {
		sync.RWMutex
		m map[string]context.CancelFunc
	}{m: make(map[string]context.CancelFunc)}
)

func RegisterJob(jobID string, cancel context.CancelFunc) {
	activeJobs.Lock()
	activeJobs.m[jobID] = cancel
	activeJobs.Unlock()
}

func UnregisterJob(jobID string) {
	activeJobs.Lock()
	delete(activeJobs.m, jobID)
	activeJobs.Unlock()
}

func StopJob(jobID string) bool {
	activeJobs.RLock()
	cancel, ok := activeJobs.m[jobID]
	activeJobs.RUnlock()
	if !ok {
		return false
	}
	cancel()
	return true
}

func IsJobRunning(jobID string) bool {
	activeJobs.RLock()
	_, ok := activeJobs.m[jobID]
	activeJobs.RUnlock()
	return ok
}

type readerFunc func([]byte) (int, error)

func (rf readerFunc) Read(p []byte) (int, error) { return rf(p) }

type fileEntry struct {
	Path         string
	ContentStart uint64
	ContentEnd   uint64
	Size         int64
}

type verifyState struct {
	fs *vfs.LocalFS
}

func (vs *verifyState) Close() error {
	if vs.fs != nil {
		return vs.fs.Close()
	}
	return nil
}

type verificationJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	task          *VerificationTask
	queueTask     *tasklog.QueuedTask
	job           database.VerificationJob
	backupJobs    []database.Backup
	storeInstance *store.Store
	web           bool

	// result counts set by execute, used by onSuccess/onError to close task
	failedFiles     int
	skippedFiles    int
	totalFiles      int
	resultID        int
	totalPopulation int
}

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

	var backups []database.Backup

	if job.TargetMode == "namespace" {
		allBackups, err := v.storeInstance.Database.GetAllBackups()
		if err != nil {
			return fmt.Errorf("failed to list backup jobs: %w", err)
		}
		for _, b := range allBackups {
			if b.Store != job.Store {
				continue
			}
			if job.Recursive {
				if job.Namespace == "" || b.Namespace == job.Namespace || strings.HasPrefix(b.Namespace, job.Namespace+"/") {
					if b.Target.IsAgent() {
						backups = append(backups, b)
					}
				}
			} else {
				if b.Namespace == job.Namespace {
					if b.Target.IsAgent() {
						backups = append(backups, b)
					}
				}
			}
		}
		if len(backups) == 0 {
			return fmt.Errorf("no agent backup jobs found in namespace '%s'", job.Namespace)
		}
	} else {
		backup, err := v.storeInstance.Database.GetBackup(job.BackupJobID)
		if err != nil {
			return fmt.Errorf("failed to get backup job %s: %w", job.BackupJobID, err)
		}
		if !backup.Target.IsAgent() {
			return ErrNotAgentTarget
		}
		backups = []database.Backup{backup}
	}

	v.mu.Lock()
	v.backupJobs = backups
	v.mu.Unlock()

	source := "schedule"
	if v.web {
		source = "web UI"
	}
	wid := tasklog.FormatWorkerID(job.Store, "host-", job.ID)
	queueTask, err := tasklog.WriteQueuedLog("pbsplusgen-queue", "verification", wid, v.web)
	if err != nil {
		log.Error(err, "failed to create queue task, not fatal")
	} else {
		v.mu.Lock()
		v.queueTask = queueTask
		v.mu.Unlock()

		if err := v.updateJobStatus(false, queueTask.Task); err != nil {
			log.Error(err, "failed to set queue task, not fatal")
		}
	}
	log.Info("verification job starting", "source", source, "jobID", job.ID)

	return nil
}

func (v *verificationJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	v.cancel = cancel

	v.mu.RLock()
	job := v.job
	backups := v.backupJobs
	v.mu.RUnlock()

	vTask, err := NewVerificationTask(job)
	if err != nil {
		return fmt.Errorf("failed to create verification task: %w", err)
	}
	v.mu.Lock()
	v.task = vTask
	v.mu.Unlock()
	if v.queueTask != nil {
		v.queueTask.Close()
	}

	if err := v.updateJobStatus(false, vTask.Task); err != nil {
		log.Error(err, "failed to update job with task UPID")
	}

	if job.TargetMode == "namespace" {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' targeting namespace '%s' (%d backup jobs)", job.ID, job.Namespace, len(backups)))
		// Jobs never verified get the highest weight; recently verified ones
		backups = weightedShuffleBackups(backups, v.storeInstance.Database, job.ID)
	} else {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' for backup job '%s'", job.ID, job.BackupJobID))
	}

	var lastStartupErr error
	for _, backup := range backups {
		snapshot, snapErr := v.selectSnapshot(ctx, job, backup)
		if snapErr != nil {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': failed to select snapshot: %v", backup.ID, snapErr))
			lastStartupErr = snapErr
			continue
		}

		hostname := backup.Target.GetHostname()
		streamID := hostname + "|" + job.ID + "|verify"

		type caller interface {
			CallMessage(ctx context.Context, method string, payload any) (string, error)
		}
		var controlSess caller
		if sess, ok := v.storeInstance.ARPCAgentsManager.GetQuicPipe(hostname); ok {
			controlSess = sess
		} else if sess, ok := v.storeInstance.ARPCAgentsManager.GetStreamPipe(hostname); ok {
			controlSess = sess
		} else {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': agent '%s' not connected", backup.ID, hostname))
			lastStartupErr = ErrAgentNotConnected
			continue
		}

		v.storeInstance.ARPCAgentsManager.Expect(streamID)

		verifyReq := verification.VerifyStartReq{VerifyID: job.ID}
		forkCtx, forkCancel := context.WithTimeout(ctx, 30*time.Second)
		_, forkErr := controlSess.CallMessage(forkCtx, "verify_start", &verifyReq)
		forkCancel()
		if forkErr != nil {
			v.storeInstance.ARPCAgentsManager.NotExpect(streamID)
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': failed to fork verification worker: %v", backup.ID, forkErr))
			lastStartupErr = forkErr
			continue
		}

		pipeCtx, pipeCancel := context.WithTimeout(ctx, 30*time.Second)
		agentTCP, waitErr := v.storeInstance.ARPCAgentsManager.WaitStreamPipe(pipeCtx, streamID)
		pipeCancel()
		if waitErr != nil {
			v.storeInstance.ARPCAgentsManager.NotExpect(streamID)
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': verification worker did not connect: %v", backup.ID, waitErr))
			lastStartupErr = waitErr
			continue
		}
		v.storeInstance.ARPCAgentsManager.NotExpect(streamID)

		vTask.WriteString(fmt.Sprintf("verification worker connected via TCP for job '%s'", backup.ID))

		vs, archiveErr := v.openArchive(backup, snapshot)
		if archiveErr != nil {
			agentTCP.Close()
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s' snapshot '%s': failed to open archive: %v", backup.ID, snapshot.Snapshot, archiveErr))
			lastStartupErr = archiveErr
			continue
		}

		vTask.WriteString(fmt.Sprintf("selected backup job '%s', snapshot: %s", backup.ID, snapshot.Snapshot))

		err := v.executeVerification(ctx, vTask, job, backup, snapshot, vs, agentTCP)
		if err == nil {
			return nil
		}

		if errors.Is(err, ErrNoFilesToVerify) && job.TargetMode == "namespace" {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': no eligible files found, trying next candidate", backup.ID))
			lastStartupErr = err
			continue
		}
		return err
	}

	if lastStartupErr != nil {
		vTask.WriteString(fmt.Sprintf("all candidates exhausted, last error: %v", lastStartupErr))
		return lastStartupErr
	}
	return fmt.Errorf("no eligible backup jobs found")
}

func (v *verificationJob) executeVerification(
	ctx context.Context,
	vTask *VerificationTask,
	job database.VerificationJob,
	backup database.Backup,
	snapshot *snapshotInfo,
	vs *verifyState,
	agentTCP *arpc.StreamPipe,
) error {
	defer func() {
		if err := vs.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	defer agentTCP.Close()

	// Create a derived context so we can cancel remaining workers on fail threshold
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	vTask.WriteString(fmt.Sprintf("selected snapshot: %s", snapshot.Snapshot))

	result := &database.VerificationResult{
		VerificationJobID: job.ID,
		UPID:              vTask.UPID(),
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

	v.mu.Lock()
	v.resultID = result.ID
	v.mu.Unlock()

	// Enumerate files from the pxar archive and sample
	sampledFiles, err := v.sampleFiles(ctx, job, vs, snapshot)
	if err != nil {
		// Mark the stale result as skipped so we don't leave orphaned "running" records
		if err := v.storeInstance.Database.MarkVerificationResultStatus(result.ID, "skipped", time.Now().Unix()); err != nil {
			log.Error(err, "")
		}
		return fmt.Errorf("failed to sample files: %w", err)
	}

	result.TotalFiles = len(sampledFiles)
	v.mu.RLock()
	result.TotalPopulation = v.totalPopulation
	v.mu.RUnlock()
	vTask.WriteString(fmt.Sprintf("sampled %d files for verification", len(sampledFiles)))

	// Verify files concurrently with a bounded worker pool.
	concurrency := 4
	if n := len(sampledFiles); n < concurrency {
		concurrency = n
	}
	if concurrency < 1 {
		concurrency = 1
	}

	type indexedResult struct {
		index  int
		result database.VerificationFileResult
	}

	filesCh := make(chan int, len(sampledFiles))
	resultsCh := make(chan indexedResult, len(sampledFiles))

	for i := range sampledFiles {
		filesCh <- i
	}
	close(filesCh)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Go(func() {
			for idx := range filesCh {
				select {
				case <-ctx.Done():
					resultsCh <- indexedResult{index: idx, result: database.VerificationFileResult{
						Path: sampledFiles[idx].Path, Size: sampledFiles[idx].Size, Status: "skipped", Message: "canceled",
					}}
					continue
				default:
				}

				fr := v.verifyFile(ctx, agentTCP, vs, sampledFiles[idx], backup)
				resultsCh <- indexedResult{index: idx, result: fr}
			}
		})
	}

	// Close results channel when all workers finish
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// workers never block on send. We drain all results to avoid goroutine leaks.
	ordered := make([]database.VerificationFileResult, len(sampledFiles))
	collected := 0
	thresholdHit := false

	for ir := range resultsCh {
		collected++
		if ir.result.Status != "" {
			ordered[ir.index] = ir.result
		}

		switch ir.result.Status {
		case "ok":
			result.VerifiedFiles++
		case "failed":
			result.FailedFiles++
			vTask.WriteString(fmt.Sprintf("file verification failed: %s - %s", ir.result.Path, ir.result.Message))
		default:
			result.SkippedFiles++
			vTask.WriteString(fmt.Sprintf("file skipped: %s - %s", ir.result.Path, ir.result.Message))
		}

		// Fail threshold: cancel remaining work (workers will see ctx.Done())
		if job.SpotConfig.FailThreshold > 0 && result.FailedFiles >= job.SpotConfig.FailThreshold && !thresholdHit {
			thresholdHit = true
			vTask.WriteString(fmt.Sprintf("fail threshold reached (%d failures), stopping verification", result.FailedFiles))
			cancel()
		}

		if collected%10 == 0 || collected == len(sampledFiles) {
			vTask.WriteString(fmt.Sprintf("progress: %d/%d files verified", collected, len(sampledFiles)))
		}
	}

	// Append collected results in order
	for _, fr := range ordered {
		if fr.Status != "" {
			result.Details = append(result.Details, fr)
		}
	}

	v.mu.Lock()
	v.totalFiles = result.TotalFiles
	v.failedFiles = result.FailedFiles
	v.skippedFiles = result.SkippedFiles
	v.mu.Unlock()

	result.CompletedAt = time.Now().Unix()
	switch {
	case result.FailedFiles > 0:
		result.Status = "warning"
	default:
		result.Status = "completed"
	}

	if err := v.storeInstance.Database.UpdateVerificationResult(*result); err != nil {
		log.Error(err, "failed to update verification result", "jobID", job.ID)
	}

	return nil
}

func (v *verificationJob) onError(err error) {
	log.Error(err, "verification job failed", "jobID", v.job.ID)

	v.mu.RLock()
	t := v.task
	rID := v.resultID
	v.mu.RUnlock()

	if rID > 0 {
		if markErr := v.storeInstance.Database.MarkVerificationResultStatus(rID, "failed", time.Now().Unix()); markErr != nil {
			log.Error(markErr, "failed to mark result as failed", "jobID", v.job.ID)
		}
	}

	if t != nil {
		t.WriteString(fmt.Sprintf("verification job error: %v", err))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		t.CloseErr(err)
	}

	if err := v.updateJobHistory(false, 0); err != nil {
		log.Error(err, "failed to update job history on error", "jobID", v.job.ID)
	}

	if v.storeInstance.BatchTracker != nil {
		v.storeInstance.BatchTracker.RecordJobResult(
			v.job.NotificationMode,
			notification.JobTypeVerification,
			v.job.ID,
			v.job.Store,
			fmt.Errorf("verification failed: %v", err),
			map[string]string{
				"namespace": v.job.Namespace,
				"succeeded": "false",
			},
		)
	}
}

func (v *verificationJob) onSuccess() {
	log.Info("verification job completed successfully", "jobID", v.job.ID)

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
			t.CloseWarn(failed)
			if err := v.updateJobHistory(true, failed); err != nil {
				log.Error(err, "failed to update job history", "jobID", v.job.ID)
			}
		} else if skipped > 0 {
			t.CloseWarn(skipped)
			if err := v.updateJobHistory(true, skipped); err != nil {
				log.Error(err, "failed to update job history", "jobID", v.job.ID)
			}
		} else {
			t.CloseOK()
		}
	}

	if err := v.updateJobHistory(true, 0); err != nil {
		log.Error(err, "failed to update job history on success", "jobID", v.job.ID)
	}

	var notifyErr error
	if failed > 0 {
		notifyErr = fmt.Errorf("verification found %d failed files", failed)
	}
	if v.storeInstance.BatchTracker != nil {
		verified := total - failed - skipped
		v.storeInstance.BatchTracker.RecordJobResult(
			v.job.NotificationMode,
			notification.JobTypeVerification,
			v.job.ID,
			v.job.Store,
			notifyErr,
			map[string]string{
				"namespace": v.job.Namespace,
				"total":     fmt.Sprintf("%d", total),
				"verified":  fmt.Sprintf("%d", verified),
				"failed":    fmt.Sprintf("%d", failed),
				"skipped":   fmt.Sprintf("%d", skipped),
				"succeeded": fmt.Sprintf("%v", failed == 0),
			},
		)
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

func (v *verificationJob) updateJobStatus(succeeded bool, task proxmox.Task) error {
	job, err := v.storeInstance.Database.GetVerificationJob(v.job.ID)
	if err != nil {
		return err
	}
	job.History.LastRunUpid = task.UPID
	job.History.LastRunStarttime = task.StartTime
	job.History.LastRunEndtime = task.EndTime
	job.History.LastRunState = task.Status
	if succeeded {
		job.History.LastSuccessfulUpid = task.UPID
	}
	return v.storeInstance.Database.UpdateVerificationJob(nil, job)
}

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
		0,
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

type snapshotInfo struct {
	Snapshot   string
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

	var idx int
	if len(filtered) > 1 {
		if rb, err := crypto.SecureRandomBytes(1); err == nil {
			idx = int(rb[0]) % len(filtered)
		} else {
			idx = mrand.Intn(len(filtered))
		}
	}
	return &filtered[idx], nil
}

func (v *verificationJob) listSnapshots(ctx context.Context, backup database.Backup) ([]snapshotInfo, error) {
	backupID := proxmox.NormalizeHostname(backup.Target.GetHostname())
	backupType := "host"

	dsInfo, err := cli.GetDatastoreInfo(backup.Store)
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

		snapFiles, err := os.ReadDir(filepath.Join(groupDir, entry.Name()))
		if err != nil {
			log.Error(err, "")
		}
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

// openArchive opens the pxar archive for the given snapshot, returning a
// verifyState that can be used for both file enumeration and content extraction.
func (v *verificationJob) openArchive(backup database.Backup, snap *snapshotInfo) (*verifyState, error) {
	dsInfo, err := cli.GetDatastoreInfo(backup.Store)
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

// sampleFiles walks the pxar archive to enumerate files, then returns a sample
// based on the configured strategy (random, systematic, or stratified).
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

	v.mu.Lock()
	v.totalPopulation = len(allFiles)
	v.mu.Unlock()

	sampleCount := job.SpotConfig.SampleCount
	if job.SpotConfig.SampleCountPercent > 0 {
		sampleCount = int(math.Ceil(float64(len(allFiles)) * job.SpotConfig.SampleCountPercent / 100))
	}
	if sampleCount <= 0 {
		sampleCount = 10
	}
	if sampleCount > len(allFiles) {
		sampleCount = len(allFiles)
	}

	strategy := job.SpotConfig.SamplingStrategy
	if strategy == "" {
		strategy = "random"
	}

	switch strategy {
	case "systematic":
		return systematicSample(allFiles, sampleCount), nil
	case "stratified":
		return stratifiedSample(allFiles, sampleCount), nil
	default: // random
		mrand.Shuffle(len(allFiles), func(i, j int) {
			allFiles[i], allFiles[j] = allFiles[j], allFiles[i]
		})
		return allFiles[:sampleCount], nil
	}
}

func systematicSample(files []fileEntry, n int) []fileEntry {
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	if n >= len(files) {
		return files
	}

	result := make([]fileEntry, n)
	step := float64(len(files)) / float64(n)
	for i := range n {
		idx := int(float64(i) * step)
		result[i] = files[idx]
	}
	return result
}

func stratifiedSample(files []fileEntry, n int) []fileEntry {
	if n >= len(files) {
		return files
	}

	groups := make(map[string][]fileEntry)
	for _, f := range files {
		dir := topLevelDir(f.Path)
		groups[dir] = append(groups[dir], f)
	}

	var result []fileEntry
	remaining := n
	groupNames := make([]string, 0, len(groups))
	for k := range groups {
		groupNames = append(groupNames, k)
	}
	sort.Strings(groupNames)

	for i, name := range groupNames {
		g := groups[name]
		var allocated int
		if i == len(groupNames)-1 {
			allocated = remaining
		} else {
			allocated = min(int(math.Round(float64(len(g))/float64(len(files))*float64(n))), remaining)
		}

		if allocated > len(g) {
			allocated = len(g)
		}

		mrand.Shuffle(len(g), func(i, j int) {
			g[i], g[j] = g[j], g[i]
		})

		result = append(result, g[:allocated]...)
		remaining -= allocated
	}

	return result
}

// topLevelDir extracts the top-level directory from a path (e.g. "/data/file.txt" → "/data").
func topLevelDir(path string) string {
	path = strings.TrimPrefix(path, "/")
	before, _, ok := strings.Cut(path, "/")
	if !ok {
		return "/"
	}
	return before
}

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
// Exclude filters take absolute precedence: if a file matches any exclude
// non-excluded files are eligible. Otherwise the file must match at least
func (v *verificationJob) matchesFilters(path string, entry *pxar.FileInfo, cfg database.SpotCheckConfig) bool {
	if len(cfg.Filters) == 0 {
		return true
	}

	var includes, excludes []database.SpotCheckFilter
	for _, f := range cfg.Filters {
		if f.FilterType == "exclude" {
			excludes = append(excludes, f)
		} else {
			includes = append(includes, f)
		}
	}

	// Exclude takes absolute precedence
	for _, filter := range excludes {
		if filterMatchesFile(path, entry, filter) {
			return false
		}
	}

	if len(includes) == 0 {
		return true
	}

	// Must match at least one include filter
	for _, filter := range includes {
		if filterMatchesFile(path, entry, filter) {
			return true
		}
	}
	return false
}

func filterMatchesFile(path string, entry *pxar.FileInfo, filter database.SpotCheckFilter) bool {
	if filter.PathPattern != "" {
		if strings.Contains(filter.PathPattern, "*") {
			matched, err := filepath.Match(filter.PathPattern, filepath.Base(path))
			if err != nil {
				log.Error(err, "")
			}
			if !matched {
				return false
			}
		} else {
			if !strings.HasPrefix(path, filter.PathPattern) {
				return false
			}
		}
	}

	if filter.MinSize > 0 && entry.Size() < filter.MinSize {
		return false
	}
	if filter.MaxSize > 0 && entry.Size() > filter.MaxSize {
		return false
	}

	return true
}

// verifyFile verifies a single file by comparing the agent's SHA-256 hash
// of the live file against the hash of the same file extracted from the
// stored pxar archive.
func (v *verificationJob) verifyFile(
	ctx context.Context,
	agentTCP *arpc.StreamPipe,
	vs *verifyState,
	file fileEntry,
	backup database.Backup,
) database.VerificationFileResult {
	result := database.VerificationFileResult{
		Path:   file.Path,
		Size:   file.Size,
		Status: "error",
	}

	rootPath := backup.Target.GetAgentHostPath()
	relPath := strings.TrimPrefix(file.Path, "/")
	if backup.Subpath != "" {
		subpath := strings.TrimPrefix(backup.Subpath, "/")
		relPath = filepath.Join(subpath, relPath)
	}
	agentPath := filepath.Join(rootPath, relPath)

	// Run agent hash and archive hash concurrently
	type agentResult struct {
		resp verification.VerifyFileResp
		err  error
	}
	type archiveResult struct {
		hash [32]byte
		err  error
	}

	agentCh := make(chan agentResult, 1)
	archiveCh := make(chan archiveResult, 1)

	go func() {
		var resp verification.VerifyFileResp
		fileCtx, fileCancel := context.WithTimeout(ctx, 5*time.Minute)
		defer fileCancel()
		err := agentTCP.Call(fileCtx, "verify_chunk_file", verification.VerifyFileReq{FilePath: agentPath}, &resp)
		agentCh <- agentResult{resp: resp, err: err}
	}()

	go func() {
		hash, err := extractFileHash(ctx, vs, file)
		archiveCh <- archiveResult{hash: hash, err: err}
	}()

	agentRes := <-agentCh
	if agentRes.err != nil {
		result.Status = "skipped"
		result.Message = fmt.Sprintf("agent call failed: %v", agentRes.err)
		<-archiveCh // drain
		return result
	}
	if agentRes.resp.Error != "" {
		result.Status = "skipped"
		result.Message = agentRes.resp.Error
		<-archiveCh // drain
		return result
	}

	archiveRes := <-archiveCh
	if archiveRes.err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("failed to extract file from archive: %v", archiveRes.err)
		return result
	}

	// Compare hashes
	if agentRes.resp.SHA256 != archiveRes.hash {
		result.Status = "failed"
		result.Message = fmt.Sprintf("SHA-256 mismatch: agent=%x, archive=%x", agentRes.resp.SHA256, archiveRes.hash)
		return result
	}

	result.Status = "ok"
	result.Message = "verified"
	return result
}

// extractFileHash reads a file's content from the pxar archive and returns
// its SHA-256 hash. It respects context cancellation.
func extractFileHash(ctx context.Context, vs *verifyState, file fileEntry) ([32]byte, error) {
	rc, err := vs.fs.ReadContentReader(file.ContentStart, file.ContentEnd)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to open content reader for [%d, %d): %w", file.ContentStart, file.ContentEnd, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	h := sha256simd.New()
	bufp := bufPool.Get().(*[]byte)
	defer bufPool.Put(bufp)

	_, err = io.CopyBuffer(h, readerFunc(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return rc.Read(p)
		}
	}), *bufp)
	if err != nil {
		return [32]byte{}, fmt.Errorf("failed to read file content: %w", err)
	}

	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}
