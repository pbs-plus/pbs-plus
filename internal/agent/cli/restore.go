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
}

type restoreSession struct {
	restoreId    string
	ctx          context.Context
	cancel       context.CancelFunc
	store        *agent.RestoreStore
	once         sync.Once
	remoteClient *pxar.Client
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

func cmdRestore(restoreId *string, srcPath *string, destPath *string, restoreMode *int) {
	if *restoreId == "" || *srcPath == "" || *destPath == "" {
		fmt.Fprintln(os.Stderr, "Error: missing required flags: restoreId, srcPath, and destPath are required")
		syslog.L.Error(errors.New("missing required flags")).WithMessage("CmdRestore: validation failed").Write()
		os.Exit(1)
	}

	if err := utils.ValidateJobId(*restoreId); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid restoreId: %v\n", err)
		syslog.L.Error(err).WithMessage("CmdRestore: restoreId validation failed").Write()
		os.Exit(1)
	}

	if err := utils.ValidateRestorePath("srcPath", *srcPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid srcPath: %v\n", err)
		syslog.L.Error(err).WithMessage("CmdRestore: srcPath validation failed").Write()
		os.Exit(1)
	}

	if err := utils.ValidateRestorePath("destPath", *destPath); err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid destPath: %v\n", err)
		syslog.L.Error(err).WithMessage("CmdRestore: destPath validation failed").Write()
		os.Exit(1)
	}

	var mode pxar.RestoreMode

	if restoreMode != nil {
		mode = pxar.RestoreMode(*restoreMode)
		switch mode {
		case pxar.RestoreModeNormal:
		case pxar.RestoreModeZip:
		default:
			mode = pxar.RestoreModeNormal
		}
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: GetEntry ServerURL failed").Write()
		os.Exit(1)
	}
	uri, err := utils.ParseURI(serverUrl.Value)
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: url.Parse failed").Write()
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("CmdRestore: GetTLSConfig failed").Write()
		os.Exit(1)
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), constants.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Plus-RestoreId", *restoreId)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	var wg sync.WaitGroup
	var restoreInitiatedOnce sync.Once

	wg.Go(func() {
		defer syslog.L.Info().WithMessage("CmdRestore: ARPC session handler shutting down").Write()

		syslog.L.Info().WithMessage("CmdRestore: Attempting connection").WithField("restoreId", *restoreId).Write()

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
					err := Restore(session, *restoreId, *srcPath, *destPath, mode)
					if err != nil {
						fmt.Fprintln(os.Stderr, "Restore failed:", err)
						syslog.L.Error(err).WithMessage("CmdRestore: Restore execution failed").Write()
						cancel()
						return
					}
					syslog.L.Info().WithMessage("CmdRestore: Restore completed successfully").Write()
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
		syslog.L.Info().WithMessage(fmt.Sprintf("CmdRestore: received signal %v", sig)).Write()
		cancel()
	}()

	wg.Wait()

	if session, ok := activeRestoreSessions.Get(*restoreId); ok {
		session.Close()
	}

	syslog.L.Info().WithMessage("CmdRestore: finished").Write()
	os.Exit(0)
}

func ExecRestore(id, srcPath, destPath string, mode int) (int, error) {
	syslog.L.Info().WithMessage("ExecRestore: begin").
		WithField("restoreId", id).
		Write()

	if err := utils.ValidateJobId(id); err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: restoreId validation failed").Write()
		return -1, fmt.Errorf("invalid restoreId: %w", err)
	}

	if err := utils.ValidateRestorePath("srcPath", srcPath); err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: srcPath validation failed").Write()
		return -1, fmt.Errorf("invalid srcPath: %w", err)
	}

	if err := utils.ValidateRestorePath("destPath", destPath); err != nil {
		syslog.L.Error(err).WithMessage("ExecRestore: destPath validation failed").Write()
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

func Restore(rpcSess *arpc.StreamPipe, restoreId, source, dest string, mode pxar.RestoreMode) error {
	syslog.L.Info().WithMessage("Restore: begin").
		WithField("restoreId", restoreId).
		Write()

	if err := utils.ValidateJobId(restoreId); err != nil {
		syslog.L.Error(err).WithMessage("Restore: restoreId validation failed").Write()
		return fmt.Errorf("invalid restoreId: %w", err)
	}

	if err := utils.ValidateRestorePath("source", source); err != nil {
		syslog.L.Error(err).WithMessage("Restore: source validation failed").Write()
		return fmt.Errorf("invalid source: %w", err)
	}

	if err := utils.ValidateRestorePath("dest", dest); err != nil {
		syslog.L.Error(err).WithMessage("Restore: dest validation failed").Write()
		return fmt.Errorf("invalid dest: %w", err)
	}

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

	err = pxar.RestoreWithOptions(session.ctx, client, []string{source}, pxar.RestoreOptions{
		DestDir: dest,
		Mode:    mode,
	})
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}
