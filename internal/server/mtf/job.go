package mtf

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tape"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/tasklog"
	"github.com/pbs-plus/pbs-plus/internal/proxmox/token"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	mtfdb "github.com/pbs-plus/pbs-plus/internal/server/mtf/store"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/tapeio"

	"github.com/pbs-plus/pbs-plus/internal/changer"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const mtfWorkerType = "backup"

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
	task        *Task
	logger      *log.Logger
	cleanupOnce sync.Once
}

func newJob(job mtfdb.MTFJob, st *store.Store, mapper *mtfdb.Mapper) *jobs.Job {
	j := &mtfJob{
		job:    job,
		store:  st,
		mapper: mapper,
		logger: log.WithScope(log.Scope{JobID: job.ID}),
	}
	return &jobs.Job{
		ID:        job.ID,
		Execute:   j.execute,
		OnSuccess: j.onSuccess,
		OnError:   j.onError,
		Cleanup:   j.cleanup,
	}
}

func NewJob(jobID string, st *store.Store) (*jobs.Job, string, error) {
	ctx := st.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	jobRec, err := st.MtfStore.GetMtfJob(ctx, jobID)
	if err != nil {
		return nil, "", err
	}

	task, err := startTask(jobRec)
	if err != nil {
		return nil, "", fmt.Errorf("start task: %w", err)
	}

	mj := &mtfJob{
		job:    jobRec,
		store:  st,
		mapper: st.MtfMapper,
		logger: log.WithScope(log.Scope{JobID: jobRec.ID}),
		task:   task,
	}

	return &jobs.Job{
		ID:        jobRec.ID,
		Execute:   mj.execute,
		OnSuccess: mj.onSuccess,
		OnError:   mj.onError,
		Cleanup:   mj.cleanup,
	}, task.UPID(), nil
}

func (j *mtfJob) execute(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	j.cancel = cancel

	task := j.task

	select {
	case <-ctx.Done():
		return jobs.ErrCanceled
	default:
	}

	cfg, err := j.buildConfig(ctx)
	if err != nil {
		task.LogString(fmt.Sprintf("MTF migration failed during setup: %s", err.Error()))
		task.CloseErr(err)
		return err
	}

	j.logger.Info("mtf job started", "job_id", j.job.ID, "source", j.job.SourceLabel, "datastore", j.job.Datastore, "upid", task.UPID())
	if err := j.persistHistory(task.Task, database.JobStatusUnknown, true); err != nil {
		j.logger.Error(err, "failed to persist MTF job history (started)")
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

	stats, runErr := tapeio.Run(ctx, cfg)
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

func (j *mtfJob) buildConfig(ctx context.Context) (tapeio.Config, error) {
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
			j.logger.Error(err, "mtf: namespace mapping failed")
			return baseNS
		}
		if mapped == "" {
			return baseNS
		}
		return mapped
	}

	cfg := tapeio.Config{
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
		j.logger.Error(err, "failed to read tape configuration")
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
			return cfg, ErrNoCartridges
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

func (j *mtfJob) configForDataSet(ctx context.Context, ds mtfdb.DataSet, cfg tapeio.Config, tapeCfg *tape.Config) (tapeio.Config, error) {
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

		if cfg.ChangerDevice != "" && len(carts) > 0 {
			if err := j.loadFirstCartridge(cfg.ChangerDevice, cfg.TapeDevice, cfg.DriveIndex, carts); err != nil {
				return cfg, fmt.Errorf("load cartridge: %w", err)
			}
		}
	}
	snaps, err := tapeio.ListSnapshots(ctx, cfg)
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
func (j *mtfJob) loadFirstCartridge(changerDev, tapeDev string, driveIdx int, carts []mtfdb.Cartridge) error {
	chg, err := changer.Open(changerDev)
	if err != nil {
		return fmt.Errorf("open changer %s: %w", changerDev, err)
	}
	defer chg.Close()

	st, err := chg.Status()
	if err != nil {
		return fmt.Errorf("changer status: %w", err)
	}

	if driveIdx < len(st.Drives) && st.Drives[driveIdx].Full {
		driveBarcode := st.Drives[driveIdx].VolumeTag
		unloadSlot := 1
		for i, s := range st.Slots {
			if s.VolumeTag == driveBarcode {
				unloadSlot = i + 1
				break
			}
		}
		if err := chg.Unload(st, driveIdx, unloadSlot); err != nil {
			return fmt.Errorf("unload drive %d: %w", driveIdx, err)
		}
	}

	barcode := carts[0].Barcode
	for i, s := range st.Slots {
		if s.Full && s.VolumeTag == barcode {
			slotIdx := i + 1
			j.logger.Info("loading cartridge into drive", "barcode", barcode, "slot", slotIdx, "drive", driveIdx)
			if err := chg.Load(st, slotIdx, driveIdx); err != nil {
				return fmt.Errorf("load slot %d into drive %d: %w", slotIdx, driveIdx, err)
			}
			return nil
		}
	}

	return fmt.Errorf("cartridge %s not found in changer slots", barcode)
}

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
	j.logger.Info("mtf job completed successfully")
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
		j.logger.Error(err, "failed to persist MTF job history on success")
	}
	j.notify(nil)
}

func (j *mtfJob) onError(runErr error) {
	j.mu.RLock()
	j.logger.Error(runErr, "mtf job failed")
	task := j.task
	job := j.job
	j.mu.RUnlock()

	if errors.Is(runErr, jobs.ErrCanceled) {
		if task != nil {
			if err := j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
				mtfdb.JobHistory{LastRunUpid: task.UPID(), LastRunStatus: database.JobStatusCanceled, LastRunEndtime: time.Now().Unix()}, ""); err != nil {
				j.logger.Error(err, "failed to update MTF job history on cancellation")
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
		j.logger.Error(err, "failed to persist MTF job history on error")
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
		j.mu.Unlock()

		if cancel != nil {
			cancel()
		}
		if logger != nil {
			logger.Close()
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
	t.WorkerTask.CloseOK()
}

func (t *Task) CloseErr(taskErr error) {
	t.WorkerTask.CloseErr(taskErr)
}

func errorTask(job mtfdb.MTFJob, runErr error) *Task {
	wt, err := tasklog.NewWorkerTask("pbsplusgen-error", mtfWorkerType, mtfWID(job))
	if err != nil {
		return nil
	}

	wt.Log("%s", runErr.Error())
	wt.CloseErr(runErr)

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
