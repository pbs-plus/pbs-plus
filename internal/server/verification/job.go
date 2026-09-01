//go:build linux

package verification

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

var (
	ErrNoSnapshots       = errors.New("no snapshots found for backup job")
	ErrNoFilesToVerify   = errors.New("no files matched filters for verification")
	ErrAgentNotConnected = errors.New("agent not connected for verification")
	ErrNotAgentTarget    = errors.New("verification requires an agent target")
)

var (
	bufPool = sync.Pool{
		New: func() any {
			buf := make([]byte, 256*1024)
			return &buf
		},
	}
)

type verificationJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	logger      *log.Logger
	task        *VerificationTask
	upid        string
	executionID string
	job         coredb.VerificationJob
	backupJobs  []coredb.Backup
	app         *application.Runtime

	// result counts set by execute, used by onSuccess/onError to close task
	failedFiles     int
	skippedFiles    int
	totalFiles      int
	resultID        int
	totalPopulation int
}

// selectCandidates picks the backup jobs to verify, in verification
// order. The weighted shuffle lives here so the durable activity
// result pins a stable order for the checkpointed verify stage.

// selectCandidates picks the backup jobs to verify, in verification
// order. The weighted shuffle lives here so the durable activity
// result pins a stable order for the checkpointed verify stage.
func (v *verificationJob) selectCandidates(ctx context.Context) []coredb.Backup {
	job := v.job
	var backups []coredb.Backup

	if job.TargetMode == "namespace" {
		allBackups, err := v.app.CoreDB.GetAllBackups()
		if err != nil {
			return nil
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
		backups = weightedShuffleBackups(backups, v.app.CoreDB, job.ID)
	} else {
		backup, err := v.app.CoreDB.GetBackup(job.BackupJobID)
		if err != nil {
			return nil
		}
		if backup.Target.IsAgent() {
			backups = []coredb.Backup{backup}
		}
	}

	v.mu.Lock()
	v.backupJobs = backups
	v.mu.Unlock()
	return backups
}

func (v *verificationJob) executeVerification(
	ctx context.Context,
	vTask *VerificationTask,
	job coredb.VerificationJob,
	backup coredb.Backup,
	snapshot *snapshotInfo,
	vs *verifyState,
	agentTCP *arpc.StreamPipe,
) error {
	defer func() {
		if err := vs.Close(); err != nil {
			v.logger.Error(err, "failed to close verify state")
		}
	}()
	defer agentTCP.Close()

	// Create a derived context so we can cancel remaining workers on fail threshold
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	vTask.WriteString(fmt.Sprintf("selected snapshot: %s", snapshot.Snapshot))

	result := &coredb.VerificationResult{
		VerificationJobID: job.ID,
		UPID:              vTask.UPID(),
		Snapshot:          snapshot.Snapshot,
		SnapshotTime:      snapshot.BackupTime,
		Status:            "running",
		StartedAt:         time.Now().Unix(),
		Details:           []coredb.VerificationFileResult{},
	}
	if err := v.app.CoreDB.CreateVerificationResult(result); err != nil {
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
		if err := v.app.CoreDB.MarkVerificationResultStatus(result.ID, "skipped", time.Now().Unix()); err != nil {
			v.logger.Error(err, "failed to mark stale verification result as skipped")
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
		result coredb.VerificationFileResult
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
					resultsCh <- indexedResult{index: idx, result: coredb.VerificationFileResult{
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
	ordered := make([]coredb.VerificationFileResult, len(sampledFiles))
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

	if err := v.app.CoreDB.UpdateVerificationResult(*result); err != nil {
		v.logger.Error(err, "failed to update verification result")
	}

	return nil
}
