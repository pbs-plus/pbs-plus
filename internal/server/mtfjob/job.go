package mtfjob

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/bkf2pxar"
	"github.com/pbs-plus/pbs-plus/internal/pbstoken"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtfstore"
	"github.com/pbs-plus/pbs-plus/internal/server/notification"
	"github.com/pbs-plus/pbs-plus/internal/server/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/server/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const mtfWorkerType = "mtf2pxar"

type mtfJob struct {
	mu     sync.RWMutex
	cancel context.CancelFunc

	job           mtfstore.MTFJob
	store         *store.Store
	mapper        *mtfstore.Mapper
	queueTaskPath string
	logger        *syslog.JobLogger
	task          proxmox.Task
	started       atomic.Bool
	cleanupOnce   sync.Once
}

func NewJob(job mtfstore.MTFJob, st *store.Store, mapper *mtfstore.Mapper, web bool) *jobs.Job {
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

// NewMtfJob loads an MTF job definition by id and builds a runnable jobs.Job.
// It is the factory wired into rpc.MtfJobFactory.
func NewMtfJob(jobID string, st *store.Store, web bool) (*jobs.Job, error) {
	ctx := st.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	j, err := st.MtfStore.GetMtfJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return NewJob(j, st, st.MtfMapper, web), nil
}

func (j *mtfJob) preExecute(web bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		ctx, cancel := context.WithCancel(ctx)
		j.cancel = cancel

		if err := j.setQueued(ctx, web); err != nil {
			syslog.L.Error(err).WithJob(j.job.ID).WithMessage("mtf: failed to set queued task").Write()
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

	task, err := startTask(j.job, "MTF migration in progress")
	if err != nil {
		return fmt.Errorf("start task: %w", err)
	}
	j.mu.Lock()
	j.task = task
	j.mu.Unlock()

	j.started.Store(true)
	if err := j.persistHistory(task, database.JobStatusUnknown, true); err != nil {
		syslog.L.Error(err).WithJob(j.job.ID).Write()
	}

	stats, runErr := bkf2pxar.Run(ctx, cfg)
	if runErr != nil {
		failTask(task, j.job, runErr, stats)
		return runErr
	}
	finishTaskOK(task, j.job, stats)
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
		vol := mtfstore.DataSetVolume{Device: device, MachineName: host}
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
		PBSURL:    "https://localhost:8007/api2/json",
		Datastore: job.Datastore,
		Namespace: baseNS,
		SkipTLS:   true,
		Verbose:   false,
		Spanning:  job.Spanning,
		OnSnapshot: func(id, ns string) {
			syslog.L.Info().WithJob(job.ID).WithMessage(fmt.Sprintf("mtf: snapshot %s -> ns=%s", id, ns)).Write()
		},
		NamespaceResolver: resolver,
	}
	if cfg.AuthToken == "" {
		cfg.AuthToken = pbstoken.ReadLocal()
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
			drive, dev, chg, idx, err := j.resolveDrive(ctx)
			if err != nil {
				return cfg, err
			}
			_ = drive
			cfg.TapeDevice = dev
			cfg.ChangerDevice = chg
			cfg.DriveIndex = idx
		}
	case "family":
		famID := mtfstore.ToInt64(job.SourceRef)
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
			drive, dev, chg, idx, err := j.resolveDrive(ctx)
			if err != nil {
				return cfg, err
			}
			_ = drive
			cfg.TapeDevice = dev
			cfg.ChangerDevice = chg
			cfg.DriveIndex = idx
		}
	case "dataset":
		ds, err := j.store.MtfStore.GetDataSet(ctx, mtfstore.ToInt64(job.SourceRef))
		if err != nil {
			return cfg, fmt.Errorf("get data set: %w", err)
		}
		return j.configForDataSet(ctx, ds, cfg)
	default:
		return cfg, fmt.Errorf("unknown source_kind %q", job.SourceKind)
	}
	return cfg, nil
}

func (j *mtfJob) configForDataSet(ctx context.Context, ds mtfstore.DataSet, cfg bkf2pxar.Config) (bkf2pxar.Config, error) {
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
		drive, dev, chg, idx, err := j.resolveDrive(ctx)
		if err != nil {
			return cfg, err
		}
		_ = drive
		cfg.TapeDevice = dev
		cfg.ChangerDevice = chg
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

func (j *mtfJob) resolveDrive(ctx context.Context) (mtfstore.Drive, string, string, int, error) {
	if j.job.Drive != "" {
		dr, err := j.store.MtfStore.GetDrive(ctx, j.job.Drive)
		if err != nil {
			return mtfstore.Drive{}, "", "", 0, err
		}
		return dr, dr.Device, dr.Changer, dr.DriveIndex, nil
	}
	drives, err := j.store.MtfStore.ListDrives(ctx)
	if err != nil || len(drives) == 0 {
		return mtfstore.Drive{}, "/dev/nst0", "", 0, nil
	}
	dr := drives[0]
	return dr, dr.Device, dr.Changer, dr.DriveIndex, nil
}

func (j *mtfJob) setQueued(ctx context.Context, web bool) error {
	task := queuedTask(j.job, web)
	file, path, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return err
	}
	source := "web UI"
	if !web {
		source = "schedule"
	}
	_, _ = fmt.Fprintf(file, "%s: TASK QUEUED: MTF job started from %s\n", tasks.Now().Format(time.RFC3339), source)
	_ = file.Close()
	j.mu.Lock()
	j.queueTaskPath = path
	j.mu.Unlock()
	return j.persistHistory(task, database.JobStatusUnknown, true)
}

func (j *mtfJob) onSuccess() {
	j.mu.RLock()
	task := j.task
	job := j.job
	j.mu.RUnlock()
	if task.UPID == "" {
		return
	}
	_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfstore.JobHistory{
			LastRunUpid:           task.UPID,
			LastRunStatus:         database.JobStatusSuccess,
			LastRunEndtime:        time.Now().Unix(),
			LastSuccessfulUpid:    task.UPID,
			LastSuccessfulEndtime: time.Now().Unix(),
		}, "")
	j.notify(nil)
}

func (j *mtfJob) onError(err error) {
	j.mu.RLock()
	task := j.task
	job := j.job
	j.mu.RUnlock()

	if errors.Is(err, jobs.ErrCanceled) {
		_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
			mtfstore.JobHistory{LastRunUpid: task.UPID, LastRunStatus: database.JobStatusCanceled, LastRunEndtime: time.Now().Unix()}, "")
		return
	}
	if task.UPID == "" {
		task = errorTask(job, err)
	}
	_ = j.store.MtfStore.UpdateMtfJobHistory(context.Background(), job.ID,
		mtfstore.JobHistory{
			LastRunUpid:    task.UPID,
			LastRunStatus:  database.JobStatusFailed,
			LastRunEndtime: time.Now().Unix(),
			RetryCount:     job.History.RetryCount + 1,
		}, "")
	j.notify(err)
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
		map[string]string{"source": j.job.SourceRef},
	)
}

func (j *mtfJob) persistHistory(task proxmox.Task, status database.JobStatus, running bool) error {
	start := task.StartTime
	if start == 0 {
		start = time.Now().Unix()
	}
	h := mtfstore.JobHistory{
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
			_ = logger.Close()
		}
	})
}

// --- synthetic UPID/task helpers ---

func mtfWID(job mtfstore.MTFJob) string {
	return proxmox.EncodeToHexEscapes(job.Datastore) +
		proxmox.EncodeToHexEscapes(":") +
		"mtf-" + proxmox.EncodeToHexEscapes(job.ID)
}

func newTask(job mtfstore.MTFJob, node string) proxmox.Task {
	startTime := tasks.Now()
	task := proxmox.Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  startTime.Unix(),
		WorkerType: mtfWorkerType,
		WID:        mtfWID(job),
		User:       proxmox.AUTH_ID,
	}
	pid := fmt.Sprintf("%08X", task.PID)
	pstart := fmt.Sprintf("%08X", task.PStart)
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	startHex := fmt.Sprintf("%08X", uint32(task.StartTime))
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:", task.Node, pid, pstart, taskID, startHex, mtfWorkerType, task.WID, proxmox.AUTH_ID)
	return task
}

func queuedTask(job mtfstore.MTFJob, web bool) proxmox.Task {
	return newTask(job, "pbsplusgen-queue")
}

func startTask(job mtfstore.MTFJob, desc string) (proxmox.Task, error) {
	task := newTask(job, "pbsplusgen-mtf")
	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return task, err
	}
	base := tasks.NewBaseTask(task, file)
	base.WriteString(desc)
	task.Status = "running"
	return task, nil
}

func finishTaskOK(task proxmox.Task, job mtfstore.MTFJob, stats *bkf2pxar.Stats) {
	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return
	}
	base := tasks.NewBaseTask(task, file)
	if stats != nil {
		base.WriteString(fmt.Sprintf("Done: %d snapshots, %d files, %d dirs", stats.Snapshots, stats.Files, stats.Dirs))
	}
	base.CloseWithStatus("OK", nil, nil)
}

func failTask(task proxmox.Task, job mtfstore.MTFJob, runErr error, stats *bkf2pxar.Stats) {
	file, _, err := tasks.CreateTaskLogFile(task.UPID)
	if err != nil {
		return
	}
	base := tasks.NewBaseTask(task, file)
	if runErr != nil {
		base.WriteString(runErr.Error())
	}
	base.CloseWithStatus("TASK ERROR: "+firstLine(runErr.Error()), nil, nil)
}

func errorTask(job mtfstore.MTFJob, err error) proxmox.Task {
	task := newTask(job, "pbsplusgen-error")
	file, _, ferr := tasks.CreateTaskLogFile(task.UPID)
	if ferr != nil {
		return task
	}
	defer func() { _ = file.Close() }()
	ts := tasks.Now().Format(time.RFC3339)
	_, _ = fmt.Fprintf(file, "%s: %s\n", ts, err.Error())
	_, _ = fmt.Fprintf(file, "%s: TASK ERROR: %s\n", ts, firstLine(err.Error()))
	tasks.WriteArchive(task.UPID, task.StartTime, firstLine(err.Error()))
	task.Status = "stopped"
	task.ExitStatus = firstLine(err.Error())
	task.EndTime = tasks.Now().Unix()
	return task
}

func firstLine(s string) string {
	if before, _, ok := strings.Cut(s, "\n"); ok {
		return strings.TrimSpace(before)
	}
	return strings.TrimSpace(s)
}
