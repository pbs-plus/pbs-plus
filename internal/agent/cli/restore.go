package cli

import (
	"bufio"
	"context"
	cryptoRand "crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containers/winquit/pkg/winquit"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/agent/registry"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/syslog"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

var (
	activeRestoreSessions *safemap.Map[string, *restoreSession]
)

func init() {
	activeRestoreSessions = safemap.New[string, *restoreSession]()
}

type restoreSession struct {
	restoreID    string
	ctx          context.Context
	cancel       context.CancelFunc
	store        *agent.RestoreStore
	once         sync.Once
	remoteClient *pxar.Client
}

func (s *restoreSession) Close() {
	syslog.L.Info().WithMessage("session: closing").WithField("restoreID", s.restoreID).Write()
	s.once.Do(func() {
		if s.remoteClient != nil {
			syslog.L.Info().WithMessage("session: closing remote client").WithField("restoreID", s.restoreID).Write()
			s.remoteClient.Close()
		}
		if s.store != nil {
			if err := s.store.EndRestore(s.restoreID); err != nil {
				syslog.L.Warn().WithMessage("session: end restore returned error").WithField("restoreID", s.restoreID).WithField("error", err.Error()).Write()
			}
		}
		activeRestoreSessions.Del(s.restoreID)
		s.cancel()
	})
	syslog.L.Info().WithMessage("session: closed").WithField("restoreID", s.restoreID).Write()
}

func cmdRestore(restoreID *string, srcPath *string, destPath *string, restoreMode *int) {
	if *restoreID == "" || *srcPath == "" || *destPath == "" {
		fmt.Fprintln(os.Stderr, "Error: missing required flags: restoreID, srcPath, and destPath are required")
		syslog.L.Error(errors.New("missing required flags")).WithMessage("restore: validation failed").Write()
		os.Exit(1)
	}

	if err := validate.ValidateJobId(*restoreID); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid restoreID: %v\n", err)
		syslog.L.Error(err).WithMessage("restore: restore id validation failed").Write()
		os.Exit(1)
	}

	if err := validate.ValidateRestorePath("srcPath", *srcPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid srcPath: %v\n", err)
		syslog.L.Error(err).WithMessage("restore: src path validation failed").Write()
		os.Exit(1)
	}

	if err := validate.ValidateRestorePath("destPath", *destPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid destPath: %v\n", err)
		syslog.L.Error(err).WithMessage("restore: dest path validation failed").Write()
		os.Exit(1)
	}

	var mode pxar.RestoreMode

	if restoreMode != nil {
		mode = pxar.RestoreMode(*restoreMode)
	} else {
		mode = pxar.RestoreModeNormal
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: GetEntry ServerURL failed").Write()
		os.Exit(1)
	}
	uri, err := agent.ParseURI(serverUrl.Value)
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: url.Parse failed").Write()
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: GetTLSConfig failed").Write()
		os.Exit(1)
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), conf.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Plus-RestoreID", *restoreID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	var wg sync.WaitGroup
	var restoreInitiatedOnce sync.Once
	restoreDone := make(chan struct{})

	wg.Go(func() {
		defer syslog.L.Info().WithMessage("restore: arpc session handler shutting down").Write()

		syslog.L.Info().WithMessage("restore: attempting connection").WithField("restoreID", *restoreID).Write()

		session, err := arpc.ConnectToServer(ctx, address, headers, tlsConfig)
		if err != nil {
			syslog.L.Error(err).WithMessage("Connection failed, exiting").Write()
			cancel()
			return
		}
		defer session.Close()

		router := arpc.NewRouter()
		router.Handle("server_ready", func(req *arpc.Request) (arpc.Response, error) {
			restoreInitiatedOnce.Do(func() {
				go func() {
					defer close(restoreDone)
					err := Restore(session, *restoreID, *srcPath, *destPath, mode)
					if err != nil {
						fmt.Fprintln(os.Stderr, "Restore failed:", err)
						syslog.L.Error(err).WithMessage("restore: execution failed").Write()
						cancel()
						return
					}
					syslog.L.Info().WithMessage("restore: completed successfully").Write()
					cancel()
				}()
			})
			return arpc.Response{Status: 200, Message: "success"}, nil
		})

		session.SetRouter(router)
		syslog.L.Info().WithMessage("ARPC connection established").Write()

		if err := session.Serve(); err != nil {
			syslog.L.Warn().WithMessage("ARPC connection lost").WithField("error", err.Error()).Write()
		}

		cancel()
	})

	go func() {
		sig := <-done
		syslog.L.Info().WithMessage(fmt.Sprintf("restore: received signal %v", sig)).Write()
		cancel()
	}()

	wg.Wait()

	// Wait for restore goroutine to complete logging before exiting
	select {
	case <-restoreDone:
		// Restore completed (success or failure), logs should be flushed
	case <-time.After(5 * time.Second):
		// Timeout waiting for restore - it may not have been initiated
	}

	if session, ok := activeRestoreSessions.Get(*restoreID); ok {
		session.Close()
	}

	syslog.L.Info().WithMessage("restore: finished").Write()
	os.Exit(0)
}

func ExecRestore(id, srcPath, destPath string, mode int) (int, error) {
	syslog.L.Info().WithMessage("restore: exec begin").
		WithField("restoreID", id).
		Write()

	if err := validate.ValidateJobId(id); err != nil {
		syslog.L.Error(err).WithMessage("restore: restore id validation failed").Write()
		return -1, fmt.Errorf("invalid restoreID: %w", err)
	}

	if err := validate.ValidateRestorePath("srcPath", srcPath); err != nil {
		syslog.L.Error(err).WithMessage("restore: src path validation failed").Write()
		return -1, fmt.Errorf("invalid srcPath: %w", err)
	}

	if err := validate.ValidateRestorePath("destPath", destPath); err != nil {
		syslog.L.Error(err).WithMessage("restore: dest path validation failed").Write()
		return -1, fmt.Errorf("invalid destPath: %w", err)
	}

	execCmd, err := os.Executable()
	if err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: os.Executable failed").Write()
		return -1, err
	}

	tokenBytes := make([]byte, 32)
	if _, err := cryptoRand.Read(tokenBytes); err != nil {
		return -1, err
	}
	token := base64.StdEncoding.EncodeToString(tokenBytes)

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-restore-%s", id))
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		return -1, err
	}

	defer func() {
		time.Sleep(5 * time.Second)
		os.Remove(tokenFile)
	}()

	args := []string{
		"--cmdMode=restore",
		"--restoreMode=" + strconv.Itoa(mode),
		"--id=" + id,
		"--srcPath=" + srcPath,
		"--destPath=" + destPath,
		"--token=" + token,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)

	cmd.Env = os.Environ()

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
	syslog.L.Info().WithMessage("restore: child process started").
		WithField("pid", cmd.Process.Pid).
		WithField("args", strings.Join(args, " ")).
		Write()

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			syslog.L.Info().
				WithField("restoreID", id).
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
				WithField("restoreID", id).
				WithField("forked", true).
				Write()
		}
		if err := errScanner.Err(); err != nil {
			syslog.L.Warn().WithMessage("ExecRestore: stderr scanner error").WithField("error", err.Error()).Write()
		}
	}()

	syslog.L.Info().WithMessage("restore: returning to parent").
		WithField("pid", cmd.Process.Pid).
		Write()
	return cmd.Process.Pid, nil
}

func Restore(rpcSess *arpc.StreamPipe, restoreID, source, dest string, mode pxar.RestoreMode) error {
	syslog.L.Info().WithMessage("restore: begin").
		WithField("restoreID", restoreID).
		Write()

	if err := validate.ValidateJobId(restoreID); err != nil {
		syslog.L.Error(err).WithMessage("restore: restore id validation failed").Write()
		return fmt.Errorf("invalid restoreID: %w", err)
	}

	if err := validate.ValidateRestorePath("source", source); err != nil {
		syslog.L.Error(err).WithMessage("Restore: source validation failed").Write()
		return fmt.Errorf("invalid source: %w", err)
	}

	if err := validate.ValidateRestorePath("dest", dest); err != nil {
		syslog.L.Error(err).WithMessage("Restore: dest validation failed").Write()
		return fmt.Errorf("invalid dest: %w", err)
	}

	store, err := agent.NewRestoreStore()
	if err != nil {
		syslog.L.Error(err).WithMessage("Restore: NewRestoreStore failed").WithField("restoreID", restoreID).Write()
		return err
	}
	if existingSession, ok := activeRestoreSessions.Get(restoreID); ok {
		syslog.L.Info().WithMessage("Restore: closing existing session").WithField("restoreID", restoreID).Write()
		existingSession.Close()
		_ = store.EndRestore(restoreID)
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &restoreSession{
		restoreID: restoreID,
		ctx:       sessionCtx,
		cancel:    cancel,
		store:     store,
	}
	activeRestoreSessions.Set(restoreID, session)

	defer session.Close()

	if hasActive, err := store.HasActiveRestoreForJob(restoreID); hasActive || err != nil {
		if err != nil {
			syslog.L.Error(err).WithMessage("Restore: HasActiveRestoreForRestore failed").WithField("restoreID", restoreID).Write()
			return err
		}
		syslog.L.Info().WithMessage("Restore: ending previous active restore").WithField("restoreID", restoreID).Write()
		_ = store.EndRestore(restoreID)
	}

	if err := store.StartRestore(restoreID); err != nil {
		syslog.L.Error(err).WithMessage("Restore: StartRestore failed").WithField("restoreID", restoreID).Write()
		return err
	}

	client := pxar.NewRemoteClient(rpcSess, restoreID)
	if client == nil {
		syslog.L.Error(errors.New("client is nil")).WithMessage("Restore: NewRemoteClient returned nil").Write()
		return fmt.Errorf("client is nil")
	}
	session.remoteClient = client

	syslog.L.Info().WithMessage("restore: client registered, session ready").
		WithField("restoreID", restoreID).
		Write()

	err = pxar.RestoreWithOptions(session.ctx, client, []string{source}, pxar.RestoreOptions{
		DestDir: dest,
		Mode:    mode,
	})
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}
