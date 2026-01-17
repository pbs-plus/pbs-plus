package forks

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/agent/snapshots"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
)

var (
	activeSessions *safemap.Map[string, *backupSession]
)

func init() {
	activeSessions = safemap.New[string, *backupSession]()
	syslog.L.Info().WithMessage("forks.init: activeSessions map initialized").Write()
}

type backupSession struct {
	backupId string
	ctx      context.Context
	cancel   context.CancelFunc
	store    *agent.BackupStore
	snapshot snapshots.Snapshot
	fs       *agentfs.AgentFSServer
	once     sync.Once
}

const BACKUP_MODE_PREFIX = "pbs-plus--child-backup-mode:"

func (s *backupSession) Close() {
	syslog.L.Info().WithMessage("backupSession.Close: begin").WithField("backupId", s.backupId).Write()
	s.once.Do(func() {
		if s.fs != nil {
			syslog.L.Info().WithMessage("backupSession.Close: closing AgentFSServer").WithField("backupId", s.backupId).Write()
			s.fs.Close()
		}
		if s.snapshot != (snapshots.Snapshot{}) && !s.snapshot.Direct && s.snapshot.Handler != nil {
			syslog.L.Info().WithMessage("backupSession.Close: deleting snapshot").WithField("backupId", s.backupId).WithField("path", s.snapshot.Path).Write()
			s.snapshot.Handler.DeleteSnapshot(s.snapshot)
		}
		if s.store != nil {
			if err := s.store.EndBackup(s.backupId); err != nil {
				syslog.L.Warn().WithMessage("backupSession.Close: EndBackup returned error").WithField("backupId", s.backupId).WithField("error", err.Error()).Write()
			}
		}
		activeSessions.Del(s.backupId)
		s.cancel()
	})
	syslog.L.Info().WithMessage("backupSession.Close: done").WithField("backupId", s.backupId).Write()
}

func cmdBackup(sourceMode, readMode, drive, backupId *string) {
	if *sourceMode == "" || *drive == "" || *backupId == "" || *readMode == "" {
		fmt.Fprintln(os.Stderr, "Error: missing required flags: sourceMode, readMode, drive, and backupId are required")
		syslog.L.Error(errors.New("missing required flags")).WithMessage("CmdBackup: validation failed").Write()
		os.Exit(1)
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		os.Exit(1)
	}
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		os.Exit(1)
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Plus-BackupId", *backupId)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	var wg sync.WaitGroup
	backupInitiated := make(chan struct{})
	var backupInitiatedOnce sync.Once

	wg.Go(func() {
		defer syslog.L.Info().WithMessage("CmdBackup: ARPC session handler shutting down").Write()

		base := 500 * time.Millisecond
		maxWait := 30 * time.Second
		factor := 2.0
		jitter := 0.2
		backoff := base

		var session *arpc.StreamPipe

		for {
			select {
			case <-ctx.Done():
				if session != nil {
					session.Close()
				}
				return
			default:
				if session == nil {
					syslog.L.Info().WithMessage("CmdBackup: Attempting connection").WithField("backupId", *backupId).Write()

					var err error
					session, err = arpc.ConnectToServer(ctx, address, headers, tlsConfig)
					if err != nil {
						if strings.Contains(err.Error(), "(code 403)") {
							syslog.L.Error(err).WithMessage("CmdBackup: Authorization failed, shutting down").Write()
							return
						}

						mult := 1 + jitter*(2*rand.Float64()-1)
						sleep := min(time.Duration(float64(backoff)*mult), maxWait)

						syslog.L.Warn().WithMessage(fmt.Sprintf("Connection failed, retrying in %v", sleep)).WithField("error", err.Error()).Write()

						select {
						case <-ctx.Done():
							return
						case <-time.After(sleep):
							backoff = min(time.Duration(float64(backoff)*factor), maxWait)
							continue
						}
					}
					session.SetRouter(arpc.NewRouter())
					syslog.L.Info().WithMessage("ARPC connection established").Write()
					backoff = base
				}

				backupInitiatedOnce.Do(func() {
					backupMode, err := Backup(session, *sourceMode, *readMode, *drive, *backupId)
					if err != nil {
						fmt.Fprintln(os.Stderr, "Backup initiation failed:", err)
						syslog.L.Error(err).WithMessage("CmdBackup: Backup initiation failed").Write()
						cancel()
						return
					}
					fmt.Println(BACKUP_MODE_PREFIX + backupMode)
					close(backupInitiated)
				})

				if err := session.Serve(); err != nil {
					syslog.L.Warn().WithMessage("ARPC connection lost, attempting recovery").WithField("error", err.Error()).Write()

					if newS, err := session.Reconnect(ctx); err == nil {
						session = newS
						syslog.L.Info().WithMessage("ARPC connection restored via Reconnect").Write()
					} else {
						session = nil // Force fresh connection on next loop iteration
					}
				} else {
					return
				}
			}
		}
	})

	go func() {
		sig := <-done
		syslog.L.Info().WithMessage(fmt.Sprintf("CmdBackup: received signal %v", sig)).Write()
		cancel()
	}()

	wg.Wait()

	if session, ok := activeSessions.Get(*backupId); ok {
		session.Close()
	}

	syslog.L.Info().WithMessage("CmdBackup: finished").Write()
	os.Exit(0)
}

func ExecBackup(sourceMode string, readMode string, drive string, backupId string) (string, int, error) {
	syslog.L.Info().WithMessage("ExecBackup: begin").
		WithField("sourceMode", sourceMode).
		WithField("readMode", readMode).
		WithField("drive", drive).
		WithField("backupId", backupId).
		Write()

	execCmd, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecBackup: os.Executable failed").Write()
		return "", -1, err
	}

	if sourceMode == "" {
		sourceMode = "snapshot"
	}

	args := []string{
		"--cmdMode=backup",
		"--sourceMode=" + sourceMode,
		"--readMode=" + readMode,
		"--drive=" + drive,
		"--backupId=" + backupId,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecBackup: StdoutPipe failed").Write()
		return "", -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecBackup: StderrPipe failed").Write()
		return "", -1, err
	}

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).WithMessage("ExecBackup: cmd.Start failed").Write()
		return "", -1, err
	}
	syslog.L.Info().WithMessage("ExecBackup: child started").
		WithField("pid", cmd.Process.Pid).
		WithField("args", strings.Join(args, " ")).
		Write()

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	backupMode := make(chan string, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if mode, found := strings.CutPrefix(line, BACKUP_MODE_PREFIX); found {
				syslog.L.Info().WithMessage("ExecBackup: detected backup mode line").WithField("mode", mode).Write()
				backupMode <- mode
			} else {
				syslog.L.Info().
					WithField("drive", drive).
					WithField("backupId", backupId).
					WithField("forked", true).
					WithMessage(line).Write()
			}
		}
		if err := scanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecBackup: stdout scanner error").WithField("error", err.Error()).Write()
		}
	}()

	go func() {
		for errScanner.Scan() {
			syslog.L.Error(errors.New(errScanner.Text())).
				WithField("drive", drive).
				WithField("backupId", backupId).
				WithField("forked", true).
				Write()
		}
		if err := errScanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecBackup: stderr scanner error").WithField("error", err.Error()).Write()
		}
	}()

	mode := strings.TrimSpace(<-backupMode)
	syslog.L.Info().WithMessage("ExecBackup: returning to parent").
		WithField("mode", mode).
		WithField("pid", cmd.Process.Pid).
		Write()
	return mode, cmd.Process.Pid, nil
}

func Backup(rpcSess *arpc.StreamPipe, sourceMode string, readMode string, drive string, backupId string) (string, error) {
	syslog.L.Info().WithMessage("Backup: begin").
		WithField("sourceMode", sourceMode).
		WithField("readMode", readMode).
		WithField("drive", drive).
		WithField("backupId", backupId).
		Write()

	store, err := agent.NewBackupStore()
	if err != nil {
		syslog.L.Error(err).WithMessage("Backup: NewBackupStore failed").WithField("backupId", backupId).Write()
		return "", err
	}
	if existingSession, ok := activeSessions.Get(backupId); ok {
		syslog.L.Info().WithMessage("Backup: closing existing session").WithField("backupId", backupId).Write()
		existingSession.Close()
		_ = store.EndBackup(backupId)
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &backupSession{
		backupId: backupId,
		ctx:      sessionCtx,
		cancel:   cancel,
		store:    store,
	}
	activeSessions.Set(backupId, session)

	if hasActive, err := store.HasActiveBackupForJob(backupId); hasActive || err != nil {
		if err != nil {
			syslog.L.Error(err).WithMessage("Backup: HasActiveBackupForJob failed").WithField("backupId", backupId).Write()
			return "", err
		}
		syslog.L.Info().WithMessage("Backup: ending previous active backup").WithField("backupId", backupId).Write()
		_ = store.EndBackup(backupId)
	}

	if err := store.StartBackup(backupId); err != nil {
		syslog.L.Error(err).WithMessage("Backup: StartBackup failed").WithField("backupId", backupId).Write()
		session.Close()
		return "", err
	}

	var snapshot snapshots.Snapshot
	backupMode := sourceMode

	if runtime.GOOS == "windows" {
		switch sourceMode {
		case "direct":
			path := drive
			volName := filepath.VolumeName(fmt.Sprintf("%s:", drive))
			path = volName + "\\"
			snapshot = snapshots.Snapshot{
				Path:        path,
				TimeStarted: time.Now(),
				SourcePath:  drive,
				Direct:      true,
			}
			syslog.L.Info().WithMessage("Backup: configured direct mode snapshot").
				WithField("path", path).
				WithField("drive", drive).
				Write()
		default:
			var err error
			snapshot, err = snapshots.Manager.CreateSnapshot(backupId, drive)
			if err != nil && snapshot == (snapshots.Snapshot{}) {
				syslog.L.Error(err).WithMessage("Backup: VSS snapshot failed; switching to direct mode").WithField("drive", drive).Write()
				backupMode = "direct"

				path := drive
				volName := filepath.VolumeName(fmt.Sprintf("%s:", drive))
				path = volName + "\\"

				snapshot = snapshots.Snapshot{
					Path:        path,
					TimeStarted: time.Now(),
					SourcePath:  drive,
					Direct:      true,
				}
			} else {
				syslog.L.Info().WithMessage("Backup: snapshot created successfully").
					WithField("path", snapshot.Path).
					WithField("drive", drive).
					Write()
			}
		}
	} else {
		snapshot = snapshots.Snapshot{
			Path:        "/",
			TimeStarted: time.Now(),
			SourcePath:  "/",
			Direct:      true,
		}
		syslog.L.Info().WithMessage("Backup: non-Windows platform, using root snapshot").Write()
	}

	session.snapshot = snapshot

	fs := agentfs.NewAgentFSServer(backupId, readMode, snapshot)
	if fs == nil {
		syslog.L.Error(errors.New("fs is nil")).WithMessage("Backup: NewAgentFSServer returned nil").Write()
		session.Close()
		return "", fmt.Errorf("fs is nil")
	}
	router := rpcSess.GetRouter()
	if router == nil {
		syslog.L.Error(errors.New("router is nil")).WithMessage("Backup: GetRouter returned nil").Write()
		return "", fmt.Errorf("router is nil")
	}

	fs.RegisterHandlers(router)
	session.fs = fs
	syslog.L.Info().WithMessage("Backup: AgentFSServer registered and session ready").
		WithField("backupId", backupId).
		WithField("mode", backupMode).
		WithField("snapshot_path", snapshot.Path).
		Write()

	return backupMode, nil
}
