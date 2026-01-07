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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
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
	restoreId    string
	ctx          context.Context
	cancel       context.CancelFunc
	store        *agent.RestoreStore
	once         sync.Once
	remoteClient *pxar.RemoteClient
}

func (s *restoreSession) Close() {
	syslog.L.Info().WithMessage("restoreSession.Close: begin").WithField("restoreId", s.restoreId).Write()
	s.once.Do(func() {
		if s.remoteClient != nil {
			syslog.L.Info().WithMessage("restoreSession.Close: closing remoteClient").WithField("restoreId", s.restoreId).Write()
			s.remoteClient.Close()
		}
		if s.store != nil {
			if err := s.store.EndRestore(s.restoreId); err != nil {
				syslog.L.Warn().WithMessage("restoreSession.Close: EndRestore returned error").WithField("restoreId", s.restoreId).WithField("error", err.Error()).Write()
			}
		}
		activeRestoreSessions.Del(s.restoreId)
		s.cancel()
	})
	syslog.L.Info().WithMessage("restoreSession.Close: done").WithField("restoreId", s.restoreId).Write()
}

// TODO: needs target as well to determine basePath of destPath
func cmdRestore(restoreId *string, srcPath *string, destPath *string) {
	if *restoreId == "" || *srcPath == "" || *destPath == "" {
		fmt.Fprintln(os.Stderr, "Error: missing required flags: restoreId, srcPath, and destPath are required")
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
	headers.Add("X-PBS-Plus-RestoreId", *restoreId)

	syslog.L.Info().WithMessage("CmdRestore: connecting to server").WithField("host", uri.Hostname()).WithField("restoreId", *restoreId).Write()
	rpcSess, err := arpc.ConnectToServer(context.Background(), fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort), headers, tlsConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to server: %v", err)
		syslog.L.Error(err).WithMessage("CmdRestore: ConnectToServer failed").Write()
		os.Exit(1)
	}

	var signalReceived atomic.Bool
	signalReceived.Store(false)

	router := arpc.NewRouter()
	router.Handle("server_ready", func(req *arpc.Request) (arpc.Response, error) {
		go func() {
			if signalReceived.Load() {
				return
			}

			signalReceived.Store(true)
			err = Restore(rpcSess, *restoreId, *srcPath, *destPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, err.Error())
				syslog.L.Error(err).WithMessage("CmdRestore: Restore failed").WithField("restoreId", *restoreId).Write()
				os.Exit(1)
			}

			syslog.L.Info().WithMessage("CmdRestore: restore goroutine finished").WithField("restoreId", *restoreId).Write()
			rpcSess.Close()

			if session, ok := activeRestoreSessions.Get(*restoreId); ok {
				session.Close()
			}

			time.Sleep(100 * time.Millisecond)
			os.Exit(0)
		}()

		return arpc.Response{
			Status:  200,
			Message: "success",
		}, nil
	})

	rpcSess.SetRouter(router)
	syslog.L.Info().WithMessage("CmdRestore: ARPC session established").WithField("restoreId", *restoreId).Write()

	var wg sync.WaitGroup
	wg.Go(func() {
		syslog.L.Info().WithMessage("CmdRestore: RPC Serve starting").WithField("restoreId", *restoreId).Write()
		if err := rpcSess.Serve(); err != nil {
			syslog.L.Error(err).WithMessage("CmdRestore: RPC Serve returned error").WithField("restoreId", *restoreId).Write()
			if session, ok := activeRestoreSessions.Get(*restoreId); ok {
				session.Close()
			}
		}
		syslog.L.Info().WithMessage("CmdRestore: RPC Serve exited").WithField("restoreId", *restoreId).Write()
	})

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	go func() {
		sig := <-done
		syslog.L.Info().WithMessage(fmt.Sprintf("CmdRestore: received signal %v, shutting down gracefully", sig)).WithField("restoreId", *restoreId).Write()

		rpcSess.Close()

		if session, ok := activeRestoreSessions.Get(*restoreId); ok {
			session.Close()
		}

		time.Sleep(100 * time.Millisecond)
		os.Exit(0)
	}()

	wg.Wait()
	syslog.L.Info().WithMessage("CmdRestore: background RPC goroutine finished").WithField("restoreId", *restoreId).Write()
	os.Exit(0)
}

func ExecRestore(id, srcPath, destPath string) (int, error) {
	syslog.L.Info().WithMessage("ExecRestore: begin").
		WithField("restoreId", id).
		Write()

	execCmd, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: os.Executable failed").Write()
		return -1, err
	}

	args := []string{
		"--cmdMode=restore",
		"--restoreId=" + id,
		"--srcPath=" + srcPath,
		"--destPath=" + destPath,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: StdoutPipe failed").Write()
		return -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: StderrPipe failed").Write()
		return -1, err
	}

	if err := cmd.Start(); err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: cmd.Start failed").Write()
		return -1, err
	}
	syslog.L.Info().WithMessage("ExecRestore: child started").
		WithField("pid", cmd.Process.Pid).
		WithField("args", strings.Join(args, " ")).
		Write()

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			syslog.L.Info().
				WithField("restoreId", id).
				WithField("forked", true).
				WithMessage(line).Write()
		}
		if err := scanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecRestore: stdout scanner error").WithField("error", err.Error()).Write()
		}
	}()

	go func() {
		for errScanner.Scan() {
			syslog.L.Error(errors.New(errScanner.Text())).
				WithField("restoreId", id).
				WithField("forked", true).
				Write()
		}
		if err := errScanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecRestore: stderr scanner error").WithField("error", err.Error()).Write()
		}
	}()

	syslog.L.Info().WithMessage("ExecRestore: returning to parent").
		WithField("pid", cmd.Process.Pid).
		Write()
	return cmd.Process.Pid, nil
}

func Restore(rpcSess *arpc.StreamPipe, restoreId, source, dest string) error {
	syslog.L.Info().WithMessage("Restore: begin").
		WithField("restoreId", restoreId).
		Write()

	store, err := agent.NewRestoreStore()
	if err != nil {
		syslog.L.Error(err).WithMessage("Restore: NewRestoreStore failed").WithField("restoreId", restoreId).Write()
		return err
	}
	if existingSession, ok := activeRestoreSessions.Get(restoreId); ok {
		syslog.L.Info().WithMessage("Restore: closing existing session").WithField("restoreId", restoreId).Write()
		existingSession.Close()
		_ = store.EndRestore(restoreId)
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &restoreSession{
		restoreId: restoreId,
		ctx:       sessionCtx,
		cancel:    cancel,
		store:     store,
	}
	activeRestoreSessions.Set(restoreId, session)

	defer session.Close()

	if hasActive, err := store.HasActiveRestoreForJob(restoreId); hasActive || err != nil {
		if err != nil {
			syslog.L.Error(err).WithMessage("Restore: HasActiveRestoreForRestore failed").WithField("restoreId", restoreId).Write()
			return err
		}
		syslog.L.Info().WithMessage("Restore: ending previous active restore").WithField("restoreId", restoreId).Write()
		_ = store.EndRestore(restoreId)
	}

	if err := store.StartRestore(restoreId); err != nil {
		syslog.L.Error(err).WithMessage("Restore: StartRestore failed").WithField("restoreId", restoreId).Write()
		return err
	}

	client := pxar.NewRemoteClient(rpcSess)
	if client == nil {
		syslog.L.Error(errors.New("client is nil")).WithMessage("Restore: NewRemoteClient returned nil").Write()
		return fmt.Errorf("client is nil")
	}
	session.remoteClient = client

	syslog.L.Info().WithMessage("Restore: client registered and session ready").
		WithField("restoreId", restoreId).
		Write()

	errs := pxar.RemoteRestore(session.ctx, client, []string{source}, dest)
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}

	return nil
}
