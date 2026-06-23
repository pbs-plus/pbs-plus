//go:build linux

package verification

import (
	"context"
	"crypto/rand"
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
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/cli"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	pxar "github.com/pbs-plus/pxar"
	"github.com/pbs-plus/pxar/datastore"
	"github.com/pbs-plus/pxar/transfer"
	"github.com/pbs-plus/pxar/vfs"
)

// weightedShuffleBackups reorders backup jobs using weighted random selection
// where the weight for each job is inversely proportional to how recently it
// was last successfully verified. Jobs that have never been verified receive
// the maximum weight. This ensures uniform coverage over successive runs and
// prevents the same backup job from being selected repeatedly.
func weightedShuffleBackups(backups []database.Backup, db *database.Database, verificationJobID string) []database.Backup {
	if len(backups) <= 1 {
		return backups
	}

	// Build a map of target hostname → last successful verification time.
	lastVerified := make(map[string]int64)
	results, err := db.GetVerificationResults(verificationJobID)
	if err == nil {
		for _, r := range results {
			if r.Status != "completed" {
				continue
			}
			// Snapshot format: host/<hostname>/<timestamp>
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

	// Repeated weighted selection without replacement.
	// Pick a random index proportional to weight, move it to the output,
	// then repeat with remaining candidates.
	remaining := make([]int, len(backups))
	for i := range remaining {
		remaining[i] = i
	}

	// Read enough crypto-random bytes for all selections.
	buf := make([]byte, len(backups)*4)
	if _, err := rand.Read(buf); err != nil {
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
		// Compute total of remaining weights
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

		// Walk remaining items until threshold is exhausted
		cumulative := 0.0
		chosen := remaining[sel] // default to first remaining
		for _, idx := range remaining[sel:] {
			cumulative += remWeights[idx]
			if cumulative >= threshold {
				chosen = idx
				break
			}
		}

		// Swap chosen into position sel
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

// RegisterJob registers a running verification job for cancellation tracking.
func RegisterJob(jobID string, cancel context.CancelFunc) {
	activeJobs.Lock()
	activeJobs.m[jobID] = cancel
	activeJobs.Unlock()
}

// UnregisterJob removes a finished verification job from tracking.
func UnregisterJob(jobID string) {
	activeJobs.Lock()
	delete(activeJobs.m, jobID)
	activeJobs.Unlock()
}

// StopJob cancels a running verification job. Returns false if not running.
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

// IsJobRunning checks if a verification job is currently running.
func IsJobRunning(jobID string) bool {
	activeJobs.RLock()
	_, ok := activeJobs.m[jobID]
	activeJobs.RUnlock()
	return ok
}

// readerFunc adapts a function to io.Reader for context-aware reads.
type readerFunc func([]byte) (int, error)

func (rf readerFunc) Read(p []byte) (int, error) { return rf(p) }

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
	backupJobs    []database.Backup // candidates (1 for backup_job mode, N for namespace mode)
	storeInstance *store.Store
	web           bool

	// result counts set by execute, used by onSuccess/onError to close task
	failedFiles     int
	skippedFiles    int
	totalFiles      int
	resultID        int
	totalPopulation int
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

	var backups []database.Backup

	if job.TargetMode == "namespace" {
		// Find all backup jobs targeting this datastore + namespace
		allBackups, err := v.storeInstance.Database.GetAllBackups()
		if err != nil {
			return fmt.Errorf("failed to list backup jobs: %w", err)
		}
		for _, b := range allBackups {
			if b.Store != job.Store {
				continue
			}
			if job.Recursive {
				// Match if backup namespace is equal to or a child of job.Namespace
				if job.Namespace == "" || b.Namespace == job.Namespace || strings.HasPrefix(b.Namespace, job.Namespace+"/") {
					if b.Target.IsAgent() {
						backups = append(backups, b)
					}
				}
			} else {
				// Exact namespace match
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
		// Single backup job mode
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
	backups := v.backupJobs
	v.mu.RUnlock()

	// Create the PBS verification task
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

	// Update job status with real task UPID
	if err := v.updateJobStatus(false, vTask.Task); err != nil {
		syslog.L.Error(err).WithMessage("failed to update job with task UPID").Write()
	}

	if job.TargetMode == "namespace" {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' targeting namespace '%s' (%d backup jobs)", job.ID, job.Namespace, len(backups)))
		// Weight backup jobs inversely to how recently they were verified.
		// Jobs never verified get the highest weight; recently verified ones
		// get the lowest. This maximises coverage over successive runs.
		backups = weightedShuffleBackups(backups, v.storeInstance.Database, job.ID)
	} else {
		vTask.WriteString(fmt.Sprintf("starting verification job '%s' for backup job '%s'", job.ID, job.BackupJobID))
	}

	// Try each backup job until one succeeds at the startup phase
	var lastStartupErr error
	for _, backup := range backups {
		snapshot, snapErr := v.selectSnapshot(ctx, job, backup)
		if snapErr != nil {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': failed to select snapshot: %v", backup.ID, snapErr))
			lastStartupErr = snapErr
			continue
		}

		// Get the agent's main control session (QUIC or TCP) to send the fork command.
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

		// Send verify_start control message to fork the worker process
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

		// Wait for the forked process to connect back via TCP
		pipeCtx, pipeCancel := context.WithTimeout(ctx, 30*time.Second)
		agentTCP, waitErr := v.storeInstance.ARPCAgentsManager.WaitStreamPipe(pipeCtx, streamID)
		pipeCancel()
		if waitErr != nil {
			v.storeInstance.ARPCAgentsManager.NotExpect(streamID)
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': verification worker did not connect: %v", backup.ID, waitErr))
			lastStartupErr = waitErr
			continue
		}
		// TCP session registered; NotExpect is no longer needed
		v.storeInstance.ARPCAgentsManager.NotExpect(streamID)

		vTask.WriteString(fmt.Sprintf("verification worker connected via TCP for job '%s'", backup.ID))

		// Try to open the archive
		vs, archiveErr := v.openArchive(backup, snapshot)
		if archiveErr != nil {
			agentTCP.Close()
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s' snapshot '%s': failed to open archive: %v", backup.ID, snapshot.Snapshot, archiveErr))
			lastStartupErr = archiveErr
			continue
		}

		vTask.WriteString(fmt.Sprintf("selected backup job '%s', snapshot: %s", backup.ID, snapshot.Snapshot))

		// Run verification for this snapshot
		err := v.executeVerification(ctx, vTask, job, backup, snapshot, vs, agentTCP)
		if err == nil {
			return nil
		}

		// In namespace mode, if no files could be sampled, try the next backup job
		if errors.Is(err, ErrNoFilesToVerify) && job.TargetMode == "namespace" {
			vTask.WriteString(fmt.Sprintf("skipping backup job '%s': no eligible files found, trying next candidate", backup.ID))
			lastStartupErr = err
			continue
		}
		return err
	}

	// All candidates exhausted
	if lastStartupErr != nil {
		vTask.WriteString(fmt.Sprintf("all candidates exhausted, last error: %v", lastStartupErr))
		return lastStartupErr
	}
	return fmt.Errorf("no eligible backup jobs found")
}

// executeVerification runs the actual file verification for a single snapshot.
func (v *verificationJob) executeVerification(
	ctx context.Context,
	vTask *VerificationTask,
	job database.VerificationJob,
	backup database.Backup,
	snapshot *snapshotInfo,
	vs *verifyState,
	agentTCP *arpc.StreamPipe,
) error {
	defer func() { _ = vs.Close() }()
	defer agentTCP.Close()

	// Create a derived context so we can cancel remaining workers on fail threshold
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	// Store result ID for onError to update
	v.mu.Lock()
	v.resultID = result.ID
	v.mu.Unlock()

	// Enumerate files from the pxar archive and sample
	sampledFiles, err := v.sampleFiles(ctx, job, vs, snapshot)
	if err != nil {
		// Mark the stale result as skipped so we don't leave orphaned "running" records
		_ = v.storeInstance.Database.MarkVerificationResultStatus(result.ID, "skipped", time.Now().Unix())
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

	// Feed file indices
	for i := range sampledFiles {
		filesCh <- i
	}
	close(filesCh)

	// Launch workers
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

	// Collect results. Since resultsCh is buffered to len(sampledFiles),
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

	// Store counts for onSuccess/onError to use when closing the task
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
		syslog.L.Error(err).WithField("jobID", job.ID).WithMessage("failed to update verification result").Write()
	}

	return nil
}

func (v *verificationJob) onError(err error) {
	syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("verification job failed").Write()

	v.mu.RLock()
	t := v.task
	rID := v.resultID
	v.mu.RUnlock()

	// Mark the result record as failed
	if rID > 0 {
		if markErr := v.storeInstance.Database.MarkVerificationResultStatus(rID, "failed", time.Now().Unix()); markErr != nil {
			syslog.L.Error(markErr).WithField("jobID", v.job.ID).WithMessage("failed to mark result as failed").Write()
		}
	}

	if t != nil {
		t.WriteString(fmt.Sprintf("verification job error: %v", err))
		t.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		t.CloseErr(err)
	}

	if err := v.updateJobHistory(false, 0); err != nil {
		syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history on error").Write()
	}

	// Send notification for verification failure
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
			t.CloseWarn(failed)
			if err := v.updateJobHistory(true, failed); err != nil {
				syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history").Write()
			}
		} else if skipped > 0 {
			t.CloseWarn(skipped)
			if err := v.updateJobHistory(true, skipped); err != nil {
				syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history").Write()
			}
		} else {
			t.CloseOK()
		}
	}

	if err := v.updateJobHistory(true, 0); err != nil {
		syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("failed to update job history on success").Write()
	}

	// Send notification for verification result
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

// updateJobStatus updates the verification job's last-run UPID in the database
// (for showing "running" / "queued" status in the UI).
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

	var idx int
	if len(filtered) > 1 {
		var b [4]byte
		if _, err := rand.Read(b[:]); err == nil {
			idx = int(b[0]) % len(filtered)
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

// --- File sampling ---

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

	// Store total population for confidence calculation
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

// systematicSample takes every k-th file after sorting by path for even coverage.
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

// stratifiedSample groups files by top-level directory and samples
// proportionally from each group.
func stratifiedSample(files []fileEntry, n int) []fileEntry {
	if n >= len(files) {
		return files
	}

	// Group by top-level directory
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
		// Proportional allocation
		var allocated int
		if i == len(groupNames)-1 {
			allocated = remaining // last group gets remainder
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
// Exclude filters take absolute precedence: if a file matches any exclude
// filter it is rejected immediately. If no include filters exist, all
// non-excluded files are eligible. Otherwise the file must match at least
// one include filter.
func (v *verificationJob) matchesFilters(path string, entry *pxar.FileInfo, cfg database.SpotCheckConfig) bool {
	if len(cfg.Filters) == 0 {
		return true
	}

	// Separate exclude and include filters
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

	// No include filters → all non-excluded files pass
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

// filterMatchesFile checks if a single filter's criteria match a file.
func filterMatchesFile(path string, entry *pxar.FileInfo, filter database.SpotCheckFilter) bool {
	if filter.PathPattern != "" {
		if strings.Contains(filter.PathPattern, "*") {
			if matched, _ := filepath.Match(filter.PathPattern, filepath.Base(path)); !matched {
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

// --- File verification ---

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

	// Compute agent path
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
	defer func() { _ = rc.Close() }()

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
