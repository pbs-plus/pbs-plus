package mtf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox/tape"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox/token"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const mtfWorkerType = "mtf2pxar"

// Task manages an MTF migration job's task log and lifecycle,
// following the same pattern as restore.RestoreTask.
type Task struct {
	tasks.BaseTask
	job mtfdb.MTFJob
}

type mtfJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	job         mtfdb.MTFJob
	store       *store.Store
	mapper      *mtfdb.Mapper
	queueTask   *tasks.QueuedTask
	task        *Task
	logger      *syslog.JobLogger
	started     atomic.Bool
	cleanupOnce sync.Once
}

func newJob(job mtfdb.MTFJob, st *store.Store, mapper *mtfdb.Mapper, web bool) *jobs.Job {
	j := &mtfJob{
		job:    job,
		store:  st,
		mapper: mapper,
		logger: syslog.NewJobLogger(job.ID),
	}
	return &jobs.Job{
		ID:        job.ID,
		PreExec:   j.preExecute(web),
		Execute:   j.execute,
		OnSuccess: j.onSuccess,
		OnError:   j.onError,
		Cleanup:   j.cleanup,
	}
}

// NewJob loads an MTF job definition by id and builds a runnable jobs.Job.
func NewJob(jobID string, st *store.Store, web bool) (*jobs.Job, error) {
	ctx := st.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	j, err := st.MtfStore.GetMtfJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return newJob(j, st, st.MtfMapper, web), nil
}

func (j *mtfJob) preExecute(web bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		_, cancel := context.WithCancel(ctx)
		j.cancel = cancel

		qt, err := tasks.GenerateMtfQueuedTask(j.job.ID, j.job.Datastore, web)
		if err != nil {
			syslog.L.Error(err).WithJob(j.job.ID).WithMessage("mtf: failed to create queue task").Write()
			return nil // non-fatal
		}
		j.queueTask = &qt

		if err := j.persistHistory(qt.Task, database.JobStatusUnknown, true); err != nil {
			syslog.L.Error(err).WithJob(j.job.ID).Write()
		}
		return nil
	}
}

func (j *mtfJob) execute(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	cfg, err := j.buildConfig(ctx)
	if err != nil {
		return err
	}

	task, err := startTask(j.job)
	if err != nil {
		return fmt.Errorf("start task: %w", err)
	}
	j.task = task

	j.started.Store(true)
	if err := j.persistHistory(task.Task, database.JobStatusUnknown, true); err != nil {
		syslog.L.Error(err).WithJob(j.job.ID).Write()
	}

	task.WriteString(fmt.Sprintf("MTF migration started: source=%s/%s datastore=%s namespace=%s",
		j.job.SourceKind, j.job.SourceRef, j.job.Datastore, j.job.Namespace))
	if j.job.Spanning {
		task.WriteString("Spanning mode: merging all cartridges of the media set")
	}
	if j.job.Changer != "" {
		task.WriteString(fmt.Sprintf("Changer: %s", j.job.Changer))
	}
	if j.job.Drive != "" {
		task.WriteString(fmt.Sprintf("Drive: %s", j.job.Drive))
	}
	task.WriteString(fmt.Sprintf("Tape device: %s", cfg.TapeDevice))
	if cfg.ChangerDevice != "" {
		task.WriteString(fmt.Sprintf("Changer device: %s", cfg.ChangerDevice))
	}

	// Wire the converter's task log to the task
	cfg.TaskLog = func(msg string) {
		task.WriteString(msg)
	}

	stats, runErr := bkf2pxar.Run(ctx, cfg)
	if runErr != nil {
		task.WriteString("Migration job summary:")
		if stats != nil {
			task.WriteString(fmt.Sprintf(" - %d snapshots", stats.Snapshots))
			task.WriteString(fmt.Sprintf(" - %d files", stats.Files))
			task.WriteString(fmt.Sprintf(" - %d dirs", stats.Dirs))
			task.WriteString(fmt.Sprintf(" - %d bytes", stats.Bytes))
		}
		task.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		task.CloseErr(runErr)
		return runErr
	}

	task.WriteString("Migration job summary:")
	if stats != nil {
		task.WriteString(fmt.Sprintf(" - %d snapshots", stats.Snapshots))
		task.WriteString(fmt.Sprintf(" - %d files", stats.Files))
		task.WriteString(fmt.Sprintf(" - %d dirs", stats.Dirs))
		task.WriteString(fmt.Sprintf(" - %d bytes", stats.Bytes))
	}
	task.WriteString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
	task.CloseOK()
	return nil
}

func (j *mtfJob) buildConfig(ctx context.Context) (bkf2pxar.Config, error) {
	job := j.job
	mapper := j.mapper

	baseNS := job.Namespace
	resolver := func(host, device string) string {
		if job.OverwriteMappings || mapper == nil {
			return baseNS
		}
		vol := mtfdb.DataSetVolume{Device: device, MachineName: host}
		mapped, err := mapper.Map(ctx, vol)
		if err != nil {
			syslog.L.Error(err).WithJob(job.ID).WithMessage("mtf: namespace mapping failed").Write()
			return baseNS
		}
		if mapped == "" {
			return baseNS
		}
		return mapped
	}

	cfg := bkf2pxar.Config{
		PBSURL:            token.DefaultAPIURL,
		Datastore:         job.Datastore,
		Namespace:         baseNS,
		SkipTLS:           true,
		Verbose:           false,
		Spanning:          job.Spanning,
		NamespaceResolver: resolver,
	}
	if cfg.AuthToken == "" {
		cfg.AuthToken = token.ReadLocal()
	}

	// Resolve changer and drive paths from PBS tape config
	tapeCfg, _ := tape.ReadConfig()

	// Resolve changer name → path
	if job.Changer != "" {
		for _, c := range tapeCfg.Changers {
			if c.Name == job.Changer {
				cfg.ChangerDevice = c.Path
				break
			}
		}
	}

	switch job.SourceKind {
	case "cartridge":
		cart, err := j.store.MtfStore.GetCartridge(ctx, job.SourceRef)
		if err != nil {
			return cfg, fmt.Errorf("get cartridge: %w", err)
		}
		if cart.IsBkfFile && cart.SourcePath != "" {
			cfg.Sources = []string{cart.SourcePath}
		} else {
			dev, chg, idx, err := j.resolveDrivePaths(tapeCfg)
			if err != nil {
				return cfg, err
			}
			cfg.TapeDevice = dev
			if cfg.ChangerDevice == "" {
				cfg.ChangerDevice = chg
			}
			cfg.DriveIndex = idx
		}
	case "family":
		famID := mtfdb.ToInt64(job.SourceRef)
		carts, err := j.store.MtfStore.ListCartridgesByFamily(ctx, famID)
		if err != nil {
			return cfg, fmt.Errorf("list cartridges: %w", err)
		}
		if len(carts) == 0 {
			return cfg, errors.New("no cartridges in media family")
		}
		allBKF := true
		for _, c := range carts {
			if !c.IsBkfFile {
				allBKF = false
				break
			}
		}
		if allBKF {
			for _, c := range carts {
				cfg.Sources = append(cfg.Sources, c.SourcePath)
			}
			cfg.Spanning = true
		} else {
			dev, chg, idx, err := j.resolveDrivePaths(tapeCfg)
			if err != nil {
				return cfg, err
			}
			cfg.TapeDevice = dev
			if cfg.ChangerDevice == "" {
				cfg.ChangerDevice = chg
			}
			cfg.DriveIndex = idx
		}
	case "dataset":
		ds, err := j.store.MtfStore.GetDataSet(ctx, mtfdb.ToInt64(job.SourceRef))
		if err != nil {
			return cfg, fmt.Errorf("get data set: %w", err)
		}
		return j.configForDataSet(ctx, ds, cfg, tapeCfg)
	default:
		return cfg, fmt.Errorf("unknown source_kind %q", job.SourceKind)
	}
	return cfg, nil
}

func (j *mtfJob) configForDataSet(ctx context.Context, ds mtfdb.DataSet, cfg bkf2pxar.Config, tapeCfg *tape.Config) (bkf2pxar.Config, error) {
	carts, err := j.store.MtfStore.ListCartridgesByFamily(ctx, ds.MediaFamilyID)
	if err != nil {
		return cfg, err
	}
	allBKF := true
	var sources []string
	for _, c := range carts {
		if !c.IsBkfFile {
			allBKF = false
			break
		}
		sources = append(sources, c.SourcePath)
	}
	if allBKF && len(sources) > 0 {
		cfg.Sources = sources
	} else {
		dev, chg, idx, err := j.resolveDrivePaths(tapeCfg)
		if err != nil {
			return cfg, err
		}
		cfg.TapeDevice = dev
		if cfg.ChangerDevice == "" {
			cfg.ChangerDevice = chg
		}
		cfg.DriveIndex = idx
	}
	snaps, err := bkf2pxar.ListSnapshots(ctx, cfg)
	if err != nil {
		return cfg, fmt.Errorf("list snapshots: %w", err)
	}
	for i, s := range snaps {
		if s.MachineName == ds.MachineName {
			cfg.SnapshotSel = i
			return cfg, nil
		}
	}
	return cfg, fmt.Errorf("data set machine %q not found in snapshots", ds.MachineName)
}

// resolveDrivePaths resolves the tape device path, changer device path,
// and drive index from the PBS tape config. It resolves changer names to
// device paths to avoid "no such file or directory" errors.
func (j *mtfJob) resolveDrivePaths(tapeCfg *tape.Config) (tapeDev, changerDev string, driveIdx int, err error) {
	if tapeCfg == nil || len(tapeCfg.Drives) == 0 {
		return "/dev/nst0", "", 0, nil
	}

	var d tape.Drive
	if j.job.Drive != "" {
		found := false
		for _, drive := range tapeCfg.Drives {
			if drive.Name == j.job.Drive {
				d = drive
				found = true
				break
			}
		}
		if !found {
			return "", "", 0, fmt.Errorf("drive %q not found in PBS config", j.job.Drive)
		}
	} else {
		d = tapeCfg.Drives[0]
	}

	tapeDev = tape.ResolveDevice(d.Path)
	driveIdx = d.ChangerDrivenum

	// Resolve changer name → device path
	if d.Changer != "" {
		for _, c := range tapeCfg.Changers {
			if c.Name == d.Changer {
				changerDev = c.Path
				break
			}
		}
	}

	return tapeDev, changerDev, driveIdx, nil
}

func (j *mtfJob) onSuccess() {
	j.mu.RLock()
	task := j.task
	job := j.job
	j.mu.RUnlock()

	if task == nil || task.UPID == "" {
		return
	}
	_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfdb.JobHistory{
			LastRunUpid:           task.UPID,
			LastRunStatus:         database.JobStatusSuccess,
			LastRunEndtime:        time.Now().Unix(),
			LastSuccessfulUpid:    task.UPID,
			LastSuccessfulEndtime: time.Now().Unix(),
		}, "")
	j.notify(nil)
}

func (j *mtfJob) onError(runErr error) {
	j.mu.RLock()
	task := j.task
	job := j.job
	j.mu.RUnlock()

	if errors.Is(runErr, jobs.ErrCanceled) {
		if task != nil {
			_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
				mtfdb.JobHistory{LastRunUpid: task.UPID, LastRunStatus: database.JobStatusCanceled, LastRunEndtime: time.Now().Unix()}, "")
		}
		return
	}

	if task == nil || task.UPID == "" {
		task = j.errorTask(runErr)
	}
	_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfdb.JobHistory{
			LastRunUpid:    task.UPID,
			LastRunStatus:  database.JobStatusFailed,
			LastRunEndtime: time.Now().Unix(),
			RetryCount:     job.History.RetryCount + 1,
		}, "")
	j.notify(runErr)
}

func (j *mtfJob) errorTask(runErr error) *Task {
	errTask := errorTask(j.job, runErr)
	return errTask
}

func (j *mtfJob) notify(err error) {
	if j.store.BatchTracker == nil {
		return
	}
	j.store.BatchTracker.RecordJobResult(
		j.job.NotificationMode,
		notification.JobTypeBackup,
		j.job.ID,
		j.job.Datastore,
		err,
		map[string]string{
			"source":      j.job.SourceRef,
			"succeeded":   fmt.Sprintf("%v", err == nil),
			"source_kind": j.job.SourceKind,
		},
	)
}

func (j *mtfJob) persistHistory(task proxmox.Task, status database.JobStatus, running bool) error {
	start := task.StartTime
	if start == 0 {
		start = time.Now().Unix()
	}
	h := mtfdb.JobHistory{
		LastRunUpid:      task.UPID,
		LastRunStatus:    status,
		LastRunStarttime: start,
	}
	if !running {
		h.LastRunEndtime = time.Now().Unix()
	}
	return j.store.MtfStore.UpdateMtfJobHistory(context.Background(), j.job.ID, h, "")
}

func (j *mtfJob) cleanup() {
	j.cleanupOnce.Do(func() {
		j.mu.Lock()
		cancel := j.cancel
		logger := j.logger
		qt := j.queueTask
		j.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if logger != nil {
			_ = logger.Close()
		}
		if qt != nil {
			qt.Close()
		}
	})
}

// --- MTF Task (matches RestoreTask pattern) ---

// startTask creates an Task, opens its log file, and registers it as active.
func startTask(job mtfdb.MTFJob) (*Task, error) {
	task := tasks.NewTask("pbsplus", mtfWorkerType, mtfWID(job))

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil, err
	}

	t := &Task{
		BaseTask: tasks.NewBaseTask(task, file),
		job:      job,
	}
	if err := tasks.AddActive(task.UPID); err != nil {
		syslog.L.Error(err).WithJob(job.ID).WithMessage("mtf: add active task").Write()
	}
	return t, nil
}

// CloseOK closes the task with "OK" status and removes from active list.
func (t *Task) CloseOK() {
	t.CloseWithStatus("OK", nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

// CloseErr closes the task with error status and removes from active list.
func (t *Task) CloseErr(taskErr error) {
	t.CloseWithStatus("TASK ERROR: "+taskErr.Error(), nil, func() {
		_ = tasks.RemoveActive(t.UPID)
	})
}

// --- Queued task ---

// errorTask creates a standalone error task file.
func errorTask(job mtfdb.MTFJob, runErr error) *Task {
	task := tasks.NewTask("pbsplusgen-error", mtfWorkerType, mtfWID(job))

	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return nil
	}

	t := &Task{
		BaseTask: tasks.NewBaseTask(task, file),
		job:      job,
	}
	t.WriteLogLine("%s", runErr.Error())
	t.WriteLogLine("TASK ERROR: %s", runErr.Error())
	_ = file.Close()
	tasks.WriteArchive(task.UPID, task.StartTime, runErr.Error())

	return t
}

func mtfWID(job mtfdb.MTFJob) string {
	return proxmox.EncodeToHexEscapes(job.Datastore) +
		proxmox.EncodeToHexEscapes(":") +
		"mtf-" + proxmox.EncodeToHexEscapes(job.ID)
}
