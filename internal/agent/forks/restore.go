package forks

import (
	"bufio"
	"context"
	"errors"
	"fmt"
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
	activeRestoreSessions *safemap.Map[string, *restoreSession]
)

func init() {
	activeRestoreSessions = safemap.New[string, *restoreSession]()
	syslog.L.Info().WithMessage("forks.init: activeRestoreSessions map initialized").Write()
}

type restoreSession struct {
	jobId    string
	ctx      context.Context
	cancel   context.CancelFunc
	store    *agent.RestoreStore
	snapshot snapshots.Snapshot
	fs       *agentfs.AgentFSServer
	once     sync.Once
}

const RESTORE_MODE_PREFIX = "pbs-plus--child-restore-mode:"

func (s *restoreSession) Close() {
	syslog.L.Info().WithMessage("restoreSession.Close: begin").WithField("jobId", s.jobId).Write()
	s.once.Do(func() {
		if s.fs != nil {
			syslog.L.Info().WithMessage("restoreSession.Close: closing AgentFSServer").WithField("jobId", s.jobId).Write()
			s.fs.Close()
		}
		if s.snapshot != (snapshots.Snapshot{}) && !s.snapshot.Direct && s.snapshot.Handler != nil {
			syslog.L.Info().WithMessage("restoreSession.Close: deleting snapshot").WithField("jobId", s.jobId).WithField("path", s.snapshot.Path).Write()
			s.snapshot.Handler.DeleteSnapshot(s.snapshot)
		}
		if s.store != nil {
			if err := s.store.EndRestore(s.jobId); err != nil {
				syslog.L.Warn().WithMessage("restoreSession.Close: EndRestore returned error").WithField("jobId", s.jobId).WithField("error", err.Error()).Write()
			}
		}
		activeRestoreSessions.Del(s.jobId)
		s.cancel()
	})
	syslog.L.Info().WithMessage("restoreSession.Close: done").WithField("jobId", s.jobId).Write()
}

func cmdRestore(sourceMode, readMode, drive, jobId *string) {
	if *sourceMode == "" || *drive == "" || *jobId == "" || *readMode == "" {
		fmt.Fprintln(os.Stderr, "Error: missing required flags: sourceMode, readMode, drive, and jobId are required")
		syslog.L.Error(errors.New("missing required flags")).WithMessage("CmdRestore: validation failed").Write()
		os.Exit(1)
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid server URL: %v", err)
		syslog.L.Error(err).WithMessage("CmdRestore: GetEntry ServerURL failed").Write()
		os.Exit(1)
	}
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid server URL: %v", err)
		syslog.L.Error(err).WithMessage("CmdRestore: url.Parse failed").Write()
		os.Exit(1)
	}

	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get TLS config for ARPC client: %v", err)
		syslog.L.Error(err).WithMessage("CmdRestore: GetTLSConfig failed").Write()
		os.Exit(1)
	}

	headers := http.Header{}
	headers.Add("X-PBS-Plus-JobId", *jobId)

	syslog.L.Info().WithMessage("CmdRestore: connecting to server").WithField("host", uri.Hostname()).WithField("jobId", *jobId).Write()
	rpcSess, err := arpc.ConnectToServer(context.Background(), fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort), headers, tlsConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to server: %v", err)
		syslog.L.Error(err).WithMessage("CmdRestore: ConnectToServer failed").Write()
		os.Exit(1)
	}
	rpcSess.SetRouter(arpc.NewRouter())
	syslog.L.Info().WithMessage("CmdRestore: ARPC session established").WithField("jobId", *jobId).Write()

	var wg sync.WaitGroup
	wg.Go(func() {
		syslog.L.Info().WithMessage("CmdRestore: RPC Serve starting").WithField("jobId", *jobId).Write()
		if err := rpcSess.Serve(); err != nil {
			syslog.L.Error(err).WithMessage("CmdRestore: RPC Serve returned error").WithField("jobId", *jobId).Write()
			if session, ok := activeRestoreSessions.Get(*jobId); ok {
				session.Close()
			}
		}
		syslog.L.Info().WithMessage("CmdRestore: RPC Serve exited").WithField("jobId", *jobId).Write()
	})

	restoreMode, err := Restore(rpcSess, *sourceMode, *readMode, *drive, *jobId)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		syslog.L.Error(err).WithMessage("CmdRestore: Restore failed").WithField("jobId", *jobId).Write()
		os.Exit(1)
	}

	fmt.Println(RESTORE_MODE_PREFIX + restoreMode)
	syslog.L.Info().WithMessage("CmdRestore: restore mode announced").WithField("mode", restoreMode).WithField("jobId", *jobId).Write()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	go func() {
		sig := <-done
		syslog.L.Info().WithMessage(fmt.Sprintf("CmdRestore: received signal %v, shutting down gracefully", sig)).WithField("jobId", *jobId).Write()

		rpcSess.Close()

		if session, ok := activeRestoreSessions.Get(*jobId); ok {
			session.Close()
		}

		time.Sleep(100 * time.Millisecond)
		os.Exit(0)
	}()

	wg.Wait()
	syslog.L.Info().WithMessage("CmdRestore: background RPC goroutine finished").WithField("jobId", *jobId).Write()
	os.Exit(0)
}

func ExecRestore(sourceMode string, readMode string, drive string, jobId string) (string, int, error) {
	syslog.L.Info().WithMessage("ExecRestore: begin").
		WithField("sourceMode", sourceMode).
		WithField("readMode", readMode).
		WithField("drive", drive).
		WithField("jobId", jobId).
		Write()

	execCmd, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: os.Executable failed").Write()
		return "", -1, err
	}

	if sourceMode == "" {
		sourceMode = "snapshot"
	}

	args := []string{
		"--cmdMode=restore",
		"--sourceMode=" + sourceMode,
		"--readMode=" + readMode,
		"--drive=" + drive,
		"--jobId=" + jobId,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: StdoutPipe failed").Write()
		return "", -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: StderrPipe failed").Write()
		return "", -1, err
	}

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: cmd.Start failed").Write()
		return "", -1, err
	}
	syslog.L.Info().WithMessage("ExecRestore: child started").
		WithField("pid", cmd.Process.Pid).
		WithField("args", strings.Join(args, " ")).
		Write()

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	restoreMode := make(chan string, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if mode, found := strings.CutPrefix(line, RESTORE_MODE_PREFIX); found {
				syslog.L.Info().WithMessage("ExecRestore: detected restore mode line").WithField("mode", mode).Write()
				restoreMode <- mode
			} else {
				syslog.L.Info().
					WithField("drive", drive).
					WithField("jobId", jobId).
					WithField("forked", true).
					WithMessage(line).Write()
			}
		}
		if err := scanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecRestore: stdout scanner error").WithField("error", err.Error()).Write()
		}
	}()

	go func() {
		for errScanner.Scan() {
			syslog.L.Error(errors.New(errScanner.Text())).
				WithField("drive", drive).
				WithField("jobId", jobId).
				WithField("forked", true).
				Write()
		}
		if err := errScanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecRestore: stderr scanner error").WithField("error", err.Error()).Write()
		}
	}()

	mode := strings.TrimSpace(<-restoreMode)
	syslog.L.Info().WithMessage("ExecRestore: returning to parent").
		WithField("mode", mode).
		WithField("pid", cmd.Process.Pid).
		Write()
	return mode, cmd.Process.Pid, nil
}

func Restore(rpcSess *arpc.StreamPipe, sourceMode string, readMode string, drive string, jobId string) (string, error) {
	syslog.L.Info().WithMessage("Restore: begin").
		WithField("sourceMode", sourceMode).
		WithField("readMode", readMode).
		WithField("drive", drive).
		WithField("jobId", jobId).
		Write()

	store, err := agent.NewRestoreStore()
	if err != nil {
		syslog.L.Error(err).WithMessage("Restore: NewRestoreStore failed").WithField("jobId", jobId).Write()
		return "", err
	}
	if existingSession, ok := activeRestoreSessions.Get(jobId); ok {
		syslog.L.Info().WithMessage("Restore: closing existing session").WithField("jobId", jobId).Write()
		existingSession.Close()
		_ = store.EndRestore(jobId)
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &restoreSession{
		jobId:  jobId,
		ctx:    sessionCtx,
		cancel: cancel,
		store:  store,
	}
	activeRestoreSessions.Set(jobId, session)

	if hasActive, err := store.HasActiveRestoreForJob(jobId); hasActive || err != nil {
		if err != nil {
			syslog.L.Error(err).WithMessage("Restore: HasActiveRestoreForJob failed").WithField("jobId", jobId).Write()
			return "", err
		}
		syslog.L.Info().WithMessage("Restore: ending previous active restore").WithField("jobId", jobId).Write()
		_ = store.EndRestore(jobId)
	}

	if err := store.StartRestore(jobId); err != nil {
		syslog.L.Error(err).WithMessage("Restore: StartRestore failed").WithField("jobId", jobId).Write()
		session.Close()
		return "", err
	}

	var snapshot snapshots.Snapshot
	restoreMode := sourceMode

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
			syslog.L.Info().WithMessage("Restore: configured direct mode snapshot").
				WithField("path", path).
				WithField("drive", drive).
				Write()
		default:
			var err error
			snapshot, err = snapshots.Manager.CreateSnapshot(jobId, drive)
			if err != nil && snapshot == (snapshots.Snapshot{}) {
				syslog.L.Error(err).WithMessage("Restore: VSS snapshot failed; switching to direct mode").WithField("drive", drive).Write()
				restoreMode = "direct"

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
				syslog.L.Info().WithMessage("Restore: snapshot created successfully").
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
		syslog.L.Info().WithMessage("Restore: non-Windows platform, using root snapshot").Write()
	}

	session.snapshot = snapshot

	fs := agentfs.NewAgentFSServer(jobId, readMode, snapshot)
	if fs == nil {
		syslog.L.Error(errors.New("fs is nil")).WithMessage("Restore: NewAgentFSServer returned nil").Write()
		session.Close()
		return "", fmt.Errorf("fs is nil")
	}
	router := rpcSess.GetRouter()
	if router == nil {
		syslog.L.Error(errors.New("router is nil")).WithMessage("Restore: GetRouter returned nil").Write()
		return "", fmt.Errorf("router is nil")
	}

	fs.RegisterHandlers(router)
	session.fs = fs
	syslog.L.Info().WithMessage("Restore: AgentFSServer registered and session ready").
		WithField("jobId", jobId).
		WithField("mode", restoreMode).
		WithField("snapshot_path", snapshot.Path).
		Write()

	return restoreMode, nil
}
