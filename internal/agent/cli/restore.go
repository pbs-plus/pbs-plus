package cli

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime/debug"
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
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/safemap"

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
	log.Info("session: closing")
	s.once.Do(func() {
		if s.remoteClient != nil {
			log.Info("session: closing remote client")
			if err := s.remoteClient.Close(); err != nil {
				log.Error(err, "session: close remote client failed")
			}
		}
		if s.store != nil {
			if err := s.store.EndRestore(s.restoreID); err != nil {
				log.Warn("session: end restore returned error", "error", err.Error())
			}
		}
		activeRestoreSessions.Del(s.restoreID)
		s.cancel()
	})
	log.Info("session: closed")
}

func cmdRestore(restoreID *string, srcPath *string, destPath *string, restoreMode *int) {
	if *restoreID == "" || *srcPath == "" || *destPath == "" {
		log.Error(errors.New("missing required flags"), "restore: validation failed")
		os.Exit(1)
	}

	if err := validate.ValidateJobId(*restoreID); err != nil {
		log.Error(err, "restore: restore id validation failed")
		os.Exit(1)
	}

	log.L = log.WithScope(log.Scope{RestoreID: *restoreID})

	if err := validate.ValidateRestorePath("srcPath", *srcPath); err != nil {
		log.Error(err, "restore: src path validation failed")
		os.Exit(1)
	}

	if err := validate.ValidateRestorePath("destPath", *destPath); err != nil {
		log.Error(err, "restore: dest path validation failed")
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
		log.Error(err, "CmdRestore: GetEntry ServerURL failed")
		os.Exit(1)
	}
	uri, err := agent.ParseURI(serverUrl.Value)
	if err != nil {
		log.Error(err, "CmdRestore: url.Parse failed")
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		log.Error(err, "CmdRestore: GetTLSConfig failed")
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
		defer log.Info("restore: arpc session handler shutting down")
		log.Info("restore: attempting connection")

		session, err := arpc.ConnectToServer(ctx, address, headers, tlsConfig)
		if err != nil {
			log.Error(err, "Connection failed, exiting")
			cancel()
			return
		}
		defer session.Close()

		router := arpc.NewRouter()
		router.Handle("server_ready", func(req *arpc.Request) (arpc.Response, error) {
			restoreInitiatedOnce.Do(func() {
				go func() {
					defer close(restoreDone)
					// Top-level panic recovery: worker panics are already recovered
					// inside restoreNormal, but this catches setup/teardown too. Log
					// the full stack to the Windows Event Log and stderr (forwarded
					// leaked .pxar-restore-* temps.
					defer func() {
						if r := recover(); r != nil {
							perr := fmt.Errorf("restore panic: %v\n%s", r, debug.Stack())
							log.Error(perr, "restore: panicked")
						}
					}()
					err := Restore(session, *restoreID, *srcPath, *destPath, mode)
					if err != nil {
						log.Error(err, "restore: execution failed")
						cancel()
						return
					}
					log.Info("restore: completed successfully")
					cancel()
				}()
			})
			return arpc.Response{Status: 200, Message: "success"}, nil
		})

		session.SetRouter(router)
		log.Info("ARPC connection established")

		if err := session.Serve(); err != nil {
			log.Warn("ARPC connection lost")
		} else {
			log.Info("ARPC session serve returned normally")
		}

		cancel()
	})

	go func() {
		sig := <-done
		log.Info(fmt.Sprintf("restore: received signal %v", sig))
		cancel()
	}()

	wg.Wait()

	// Wait for restore goroutine to complete logging before exiting
	select {
	case <-restoreDone:
		// Restore completed (success or failure), logs should be flushed
	case <-time.After(5 * time.Second):
	}

	if session, ok := activeRestoreSessions.Get(*restoreID); ok {
		session.Close()
	}
	log.Info("restore: finished")
	os.Exit(0)
}

func ExecRestore(id, srcPath, destPath string, mode int) (int, error) {
	log.Info("restore: exec begin")
	if err := validate.ValidateJobId(id); err != nil {
		log.Error(err, "restore: restore id validation failed")
		return -1, fmt.Errorf("invalid restoreID: %w", err)
	}

	if err := validate.ValidateRestorePath("srcPath", srcPath); err != nil {
		log.Error(err, "restore: src path validation failed")
		return -1, fmt.Errorf("invalid srcPath: %w", err)
	}

	if err := validate.ValidateRestorePath("destPath", destPath); err != nil {
		log.Error(err, "restore: dest path validation failed")
		return -1, fmt.Errorf("invalid destPath: %w", err)
	}

	execCmd, err := os.Executable()
	if err != nil {
		log.Error(err, "ExecRestore: os.Executable failed")
		return -1, err
	}

	tokenBytes, err := crypto.SecureRandomBytes(32)
	if err != nil {
		return -1, err
	}
	token := base64.StdEncoding.EncodeToString(tokenBytes)

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-restore-%s", id))
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		return -1, err
	}

	defer func() {
		time.Sleep(5 * time.Second)
		if err := os.Remove(tokenFile); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
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
		log.Error(err, "ExecRestore: StdoutPipe failed")
		return -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Error(err, "ExecRestore: StderrPipe failed")
		return -1, err
	}

	if err := cmd.Start(); err != nil {
		log.Error(err, "ExecRestore: cmd.Start failed")
		return -1, err
	}
	log.Info("restore: child process started",

		"args", strings.Join(args, " "), "pid", cmd.Process.Pid)

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			log.Info(line, "forked", true)

		}
		if err := scanner.Err(); err != nil {
			log.Warn("ExecRestore: stdout scanner error", "error", err.Error())
		}
	}()

	go func() {
		for errScanner.Scan() {
			log.Error(errors.New(errScanner.Text()), "", "forked", true)

		}
		if err := errScanner.Err(); err != nil {
			log.Warn("ExecRestore: stderr scanner error", "error", err.Error())
		}
	}()
	log.Info("restore: returning to parent",
		"pid", cmd.Process.Pid)

	return cmd.Process.Pid, nil
}

func Restore(rpcSess *arpc.StreamPipe, restoreID, source, dest string, mode pxar.RestoreMode) error {
	log.Info("restore: begin")
	if err := validate.ValidateJobId(restoreID); err != nil {
		log.Error(err, "restore: restore id validation failed")
		return fmt.Errorf("invalid restoreID: %w", err)
	}

	if err := validate.ValidateRestorePath("source", source); err != nil {
		log.Error(err, "Restore: source validation failed")
		return fmt.Errorf("invalid source: %w", err)
	}

	if err := validate.ValidateRestorePath("dest", dest); err != nil {
		log.Error(err, "Restore: dest validation failed")
		return fmt.Errorf("invalid dest: %w", err)
	}

	store, err := agent.NewRestoreStore()
	if err != nil {
		log.Error(err, "Restore: NewRestoreStore failed")
		return err
	}
	if existingSession, ok := activeRestoreSessions.Get(restoreID); ok {
		log.Info("Restore: closing existing session")
		existingSession.Close()
		if err := store.EndRestore(restoreID); err != nil {
			log.Error(err, "")
		}
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
			log.Error(err, "Restore: HasActiveRestoreForRestore failed")
			return err
		}
		log.Info("Restore: ending previous active restore")
		if err := store.EndRestore(restoreID); err != nil {
			log.Error(err, "")
		}
	}

	if err := store.StartRestore(restoreID); err != nil {
		log.Error(err, "Restore: StartRestore failed")
		return err
	}

	client := pxar.NewRemoteClient(rpcSess, restoreID)
	if client == nil {
		log.Error(errors.New("client is nil"), "Restore: NewRemoteClient returned nil")
		return fmt.Errorf("client is nil")
	}
	session.remoteClient = client
	log.Info("restore: client registered, session ready")
	err = pxar.RestoreWithOptions(session.ctx, client, []string{source}, pxar.RestoreOptions{
		DestDir: dest,
		Mode:    mode,
	})
	if err != nil {
		return fmt.Errorf("%v", err)
	}

	return nil
}
