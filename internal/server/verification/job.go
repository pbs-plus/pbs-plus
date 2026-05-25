//go:build linux

package verification

import (
	"context"
	"errors"
	"fmt"
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

const defaultAvgChunkSize = 64 * 1024 // 64KB, matches PBS default for pxar

// fileEntry represents a file found in the pxar archive.
type fileEntry struct {
	Path         string
	ContentStart uint64
	ContentEnd   uint64
	Size         int64
}

// verificationJob holds state for a verification run.
type verificationJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	job           database.VerificationJob
	backupJob     database.Backup
	storeInstance *store.Store
}

// NewVerificationJob creates a new verification job.
func NewVerificationJob(
	job database.VerificationJob,
	storeInstance *store.Store,
) (*jobs.Job, error) {
	v := &verificationJob{
		job:           job,
		storeInstance: storeInstance,
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

	return nil
}

func (v *verificationJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	v.cancel = cancel

	v.mu.RLock()
	job := v.job
	backup := v.backupJob
	v.mu.RUnlock()

	// Select snapshot
	snapshot, err := v.selectSnapshot(ctx, job, backup)
	if err != nil {
		return fmt.Errorf("failed to select snapshot: %w", err)
	}

	// Create result record
	result := &database.VerificationResult{
		VerificationJobID: job.ID,
		Snapshot:          snapshot.Snapshot,
		SnapshotTime:      snapshot.BackupTime,
		Status:            "running",
		StartedAt:         time.Now().Unix(),
		Details:           []database.VerificationFileResult{},
	}
	if err := v.storeInstance.Database.CreateVerificationResult(result); err != nil {
		return fmt.Errorf("failed to create verification result: %w", err)
	}

	// Read the dynamic index from the snapshot
	didx, err := v.readDynamicIndex(backup, snapshot)
	if err != nil {
		return fmt.Errorf("failed to read dynamic index: %w", err)
	}

	// Enumerate files from the pxar archive and sample
	sampledFiles, err := v.sampleFiles(ctx, job, backup, snapshot)
	if err != nil {
		return fmt.Errorf("failed to sample files: %w", err)
	}

	result.TotalFiles = len(sampledFiles)

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
	for _, file := range sampledFiles {
		select {
		case <-ctx.Done():
			return jobs.ErrCanceled
		default:
		}

		fileResult := v.verifyFile(ctx, agentCaller, didx, file, backup)
		result.Details = append(result.Details, fileResult)

		switch fileResult.Status {
		case "ok":
			result.VerifiedFiles++
		case "failed":
			result.FailedFiles++
		default:
			result.SkippedFiles++
		}
	}

	result.Status = "completed"
	result.CompletedAt = time.Now().Unix()

	if err := v.storeInstance.Database.UpdateVerificationResult(*result); err != nil {
		syslog.L.Error(err).WithField("jobID", job.ID).WithMessage("failed to update verification result").Write()
	}

	return nil
}

func (v *verificationJob) onError(err error) {
	syslog.L.Error(err).WithField("jobID", v.job.ID).WithMessage("verification job failed").Write()
}

func (v *verificationJob) onSuccess() {
	syslog.L.Info().WithField("jobID", v.job.ID).WithMessage("verification job completed successfully").Write()
}

func (v *verificationJob) cleanup() {
	if v.cancel != nil {
		v.cancel()
	}
}

// snapshotInfo represents a resolved snapshot.
type snapshotInfo struct {
	Snapshot   string // "type/id/time"
	BackupType string
	BackupID   string
	BackupTime int64 // unix timestamp
	Files      []string
}

// selectSnapshot picks a snapshot based on the verification job config.
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

	// Filter by date range if specified
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
			continue // skip non-timestamp directories
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

// readDynamicIndex reads the .didx file(s) from the snapshot.
func (v *verificationJob) readDynamicIndex(backup database.Backup, snap *snapshotInfo) (*datastore.DynamicIndexReader, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(backup.Store)
	if err != nil {
		return nil, fmt.Errorf("failed to get datastore info: %w", err)
	}

	t := time.Unix(snap.BackupTime, 0).UTC()
	snapshotTime := t.Format(time.RFC3339)

	mpxarPath, ppxarPath, isSplit, err := proxmox.BuildPxarPaths(
		dsInfo.Path,
		backup.Namespace,
		snap.BackupType,
		snap.BackupID,
		snapshotTime,
		"",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build pxar paths: %w", err)
	}

	var didxPath string
	if isSplit {
		didxPath = ppxarPath
	} else {
		didxPath = mpxarPath
	}

	didxData, err := os.ReadFile(didxPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read dynamic index %s: %w", didxPath, err)
	}

	return datastore.ParseDynamicIndex(didxData)
}

// sampleFiles enumerates files from the pxar archive and returns a random sample.
func (v *verificationJob) sampleFiles(ctx context.Context, job database.VerificationJob, backup database.Backup, snap *snapshotInfo) ([]fileEntry, error) {
	dsInfo, err := proxmox.GetDatastoreInfo(backup.Store)
	if err != nil {
		return nil, err
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
		// Non-split archive
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
	defer func() { _ = fs.Close() }()

	root, err := fs.Root()
	if err != nil {
		return nil, fmt.Errorf("failed to get archive root: %w", err)
	}

	var allFiles []fileEntry
	allFiles, err = v.walkDir(fs, root, "", allFiles, job.SpotConfig)
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

// verifyFile verifies a single file by comparing agent chunk digests with stored ones.
func (v *verificationJob) verifyFile(
	ctx context.Context,
	agentCaller interface {
		Call(ctx context.Context, method string, payload any, out any) error
	},
	didx *datastore.DynamicIndexReader,
	file fileEntry,
	backup database.Backup,
) database.VerificationFileResult {
	result := database.VerificationFileResult{
		Path:   file.Path,
		Status: "error",
	}

	// 1. Ask agent to chunk the file and return digests
	// Build the local file path on the agent's filesystem.
	// file.Path is relative to the backup root (e.g., "/random1.txt").
	// backup.Subpath is the subpath within the mount point (e.g., "/test-backup").
	// target.GetAgentHostPath() returns the root path on the agent (e.g., "C:\\" or "/").
	rootPath := backup.Target.GetAgentHostPath()
	relPath := strings.TrimPrefix(file.Path, "/")
	if backup.Subpath != "" {
		subpath := strings.TrimPrefix(backup.Subpath, "/")
		relPath = filepath.Join(subpath, relPath)
	}
	agentPath := filepath.Join(rootPath, relPath)

	req := verification.VerifyFileReq{
		FilePath:     agentPath,
		AvgChunkSize: defaultAvgChunkSize,
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

	// 2. Get stored chunk digests for the file's content range
	storedDigests := getStoredDigestsForRange(didx, file.ContentStart, file.ContentEnd)

	// 3. Compare
	if len(agentResp.Digests) != len(storedDigests) {
		result.Status = "failed"
		result.Message = fmt.Sprintf("chunk count mismatch: agent=%d, stored=%d", len(agentResp.Digests), len(storedDigests))
		return result
	}

	for i, agentDigest := range agentResp.Digests {
		if i >= len(storedDigests) {
			result.Status = "failed"
			result.Message = fmt.Sprintf("agent has more chunks than stored (chunk %d)", i)
			return result
		}
		if agentDigest.Digest != storedDigests[i].Digest {
			result.Status = "failed"
			result.Message = fmt.Sprintf("chunk %d digest mismatch", i)
			return result
		}
		if agentDigest.Size != storedDigests[i].Size {
			result.Status = "failed"
			result.Message = fmt.Sprintf("chunk %d size mismatch: agent=%d, stored=%d", i, agentDigest.Size, storedDigests[i].Size)
			return result
		}
	}

	result.Status = "ok"
	result.Message = "verified"
	return result
}

// getStoredDigestsForRange returns chunk digests from the dynamic index
// that fall within the given byte range.
func getStoredDigestsForRange(didx *datastore.DynamicIndexReader, contentStart, contentEnd uint64) []verification.ChunkDigest {
	startIdx, ok := didx.ChunkFromOffset(contentStart)
	if !ok {
		return nil
	}

	var result []verification.ChunkDigest
	for i := startIdx; i < didx.Count(); i++ {
		info, ok := didx.ChunkInfo(i)
		if !ok {
			break
		}
		if info.Start >= contentEnd {
			break
		}
		result = append(result, verification.ChunkDigest{
			Digest: info.Digest,
			Size:   int(info.End - info.Start),
		})
	}

	return result
}
