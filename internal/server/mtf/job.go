package mtf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tape"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	tasks "github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const mtfWorkerType = "mtf2pxar"

type Task struct {
	*tasklog.WorkerTask
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

	task.LogString(fmt.Sprintf("MTF migration started: source=%s/%s datastore=%s namespace=%s",
		j.job.SourceKind, j.job.SourceRef, j.job.Datastore, j.job.Namespace))
	if j.job.Spanning {
		task.LogString("Spanning mode: merging all cartridges of the media set")
	}
	if j.job.Changer != "" {
		task.LogString(fmt.Sprintf("Changer: %s", j.job.Changer))
	}
	if j.job.Drive != "" {
		task.LogString(fmt.Sprintf("Drive: %s", j.job.Drive))
	}
	task.LogString(fmt.Sprintf("Tape device: %s", cfg.TapeDevice))
	if cfg.ChangerDevice != "" {
		task.LogString(fmt.Sprintf("Changer device: %s", cfg.ChangerDevice))
	}

	cfg.TaskLog = func(msg string) {
		task.LogString(msg)
	}

	stats, runErr := bkf2pxar.Run(ctx, cfg)
	if runErr != nil {
		task.LogString("Migration job summary:")
		if stats != nil {
			task.LogString(fmt.Sprintf(" - %d snapshots", stats.Snapshots))
			task.LogString(fmt.Sprintf(" - %d files", stats.Files))
			task.LogString(fmt.Sprintf(" - %d dirs", stats.Dirs))
			task.LogString(fmt.Sprintf(" - %d bytes", stats.Bytes))
		}
		task.LogString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
		task.CloseErr(runErr)
		return runErr
	}

	task.LogString("Migration job summary:")
	if stats != nil {
		task.LogString(fmt.Sprintf(" - %d snapshots", stats.Snapshots))
		task.LogString(fmt.Sprintf(" - %d files", stats.Files))
		task.LogString(fmt.Sprintf(" - %d dirs", stats.Dirs))
		task.LogString(fmt.Sprintf(" - %d bytes", stats.Bytes))
	}
	task.LogString(fmt.Sprintf("End Time: %s", time.Now().Format("Mon Jan 2 15:04:05 2006")))
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

	tapeCfg, err := tape.ReadConfig()
	if err != nil {
		syslog.L.Error(err).Write()
	}

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

	if task == nil || task.UPID() == "" {
		return
	}
	if err := j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfdb.JobHistory{
			LastRunUpid:           task.UPID(),
			LastRunStatus:         database.JobStatusSuccess,
			LastRunEndtime:        time.Now().Unix(),
			LastSuccessfulUpid:    task.UPID(),
			LastSuccessfulEndtime: time.Now().Unix(),
		}, ""); err != nil {
		syslog.L.Error(err).Write()
	}
	j.notify(nil)
}

func (j *mtfJob) onError(runErr error) {
	j.mu.RLock()
	task := j.task
	job := j.job
	j.mu.RUnlock()

	if errors.Is(runErr, jobs.ErrCanceled) {
		if task != nil {
			if err := j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
				mtfdb.JobHistory{LastRunUpid: task.UPID(), LastRunStatus: database.JobStatusCanceled, LastRunEndtime: time.Now().Unix()}, ""); err != nil {
				syslog.L.Error(err).Write()
			}
		}
		return
	}

	if task == nil || task.UPID() == "" {
		task = j.errorTask(runErr)
	}
	if err := j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfdb.JobHistory{
			LastRunUpid:    task.UPID(),
			LastRunStatus:  database.JobStatusFailed,
			LastRunEndtime: time.Now().Unix(),
			RetryCount:     job.History.RetryCount + 1,
		}, ""); err != nil {
		syslog.L.Error(err).Write()
	}
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
			if err := logger.Close(); err != nil {
				syslog.L.Error(err).Write()
			}
		}
		if qt != nil {
			qt.Close()
		}
	})
}

func startTask(job mtfdb.MTFJob) (*Task, error) {
	wt, err := tasklog.NewWorkerTask("pbsplus", mtfWorkerType, mtfWID(job))
	if err != nil {
		return nil, err
	}

	return &Task{
		WorkerTask: wt,
		job:        job,
	}, nil
}

func (t *Task) CloseOK() {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusOK, EndTime: time.Now().Unix()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func (t *Task) CloseErr(taskErr error) {
	t.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusError, EndTime: time.Now().Unix(), Message: taskErr.Error()}, func() {
		if err := tasklog.RemoveActive(t.UPID()); err != nil {
			syslog.L.Error(err).Write()
		}
	})
}

func errorTask(job mtfdb.MTFJob, runErr error) *Task {
	wt, err := tasklog.NewWorkerTask("pbsplusgen-error", mtfWorkerType, mtfWID(job))
	if err != nil {
		return nil
	}

	wt.Log("%s", runErr.Error())
	wt.CloseWithStatus(tasklog.TaskState{Status: tasklog.StatusError, EndTime: time.Now().Unix(), Message: runErr.Error()}, nil)

	return &Task{
		WorkerTask: wt,
		job:        job,
	}
}

func mtfWID(job mtfdb.MTFJob) string {
	return proxmox.EncodeToHexEscapes(job.Datastore) +
		proxmox.EncodeToHexEscapes(":") +
		"mtf-" + proxmox.EncodeToHexEscapes(job.ID)
}
