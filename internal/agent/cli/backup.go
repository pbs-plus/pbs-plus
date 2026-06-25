package cli

import (
	"bufio"
	"context"
	"encoding/base64"
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
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/safemap"

	"github.com/pbs-plus/pbs-plus/internal/validate"
)

var (
	activeSessions *safemap.Map[string, *backupSession]
)

func init() {
	activeSessions = safemap.New[string, *backupSession]()
}

type backupSession struct {
	backupID string
	ctx      context.Context
	cancel   context.CancelFunc
	store    *agent.BackupStore
	snapshot snapshots.Snapshot
	fs       *agentfs.AgentFSServer
	once     sync.Once
}

const BACKUP_MODE_PREFIX = "pbs-plus--child-backup-mode:"

func (s *backupSession) Close() {
	log.Info("session: closing")
	s.once.Do(func() {
		if s.fs != nil {
			log.Info("session: closing agentfs server")
			s.fs.Close()
		}
		if s.snapshot != (snapshots.Snapshot{}) && !s.snapshot.Direct && s.snapshot.Handler != nil {
			log.Info("session: deleting snapshot", "path", s.snapshot.Path)
			if err := s.snapshot.Handler.DeleteSnapshot(s.snapshot); err != nil {
				log.Error(err, "session: delete snapshot failed")
			}
		}
		if s.store != nil {
			if err := s.store.EndBackup(s.backupID); err != nil {
				log.Warn("session: end backup returned error", "error", err.Error())
			}
		}
		activeSessions.Del(s.backupID)
		s.cancel()
	})
	log.Info("session: closed")
}

func cmdBackup(sourceMode, readMode, drive, backupID *string) {
	if *sourceMode == "" || *drive == "" || *backupID == "" || *readMode == "" {
		log.Error(errors.New("missing required flags"), "backup: validation failed")
		os.Exit(1)
	}

	if err := validate.ValidateJobId(*backupID); err != nil {
		log.Error(err, "backup: backup id validation failed")
		os.Exit(1)
	}

	log.L = log.WithScope(log.Scope{BackupID: *backupID})

	validSourceModes := map[string]bool{"snapshot": true, "direct": true}
	if !validSourceModes[*sourceMode] {
		log.Error(errors.New("invalid sourceMode"), "CmdBackup: sourceMode validation failed")
		os.Exit(1)
	}

	validReadModes := map[string]bool{"standard": true, "mmap": true}
	if !validReadModes[*readMode] {
		log.Error(errors.New("invalid readMode"), "CmdBackup: readMode validation failed")
		os.Exit(1)
	}

	serverUrl, err := registry.GetEntry(registry.CONFIG, "ServerURL", false)
	if err != nil {
		os.Exit(1)
	}
	uri, err := agent.ParseURI(serverUrl.Value)
	if err != nil {
		os.Exit(1)
	}
	tlsConfig, err := agent.GetTLSConfig()
	if err != nil {
		os.Exit(1)
	}

	address := fmt.Sprintf("%s%s", strings.TrimSuffix(uri.Hostname(), ":"), conf.ARPCServerPort)
	headers := http.Header{}
	headers.Add("X-PBS-Plus-BackupID", *backupID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	winquit.SimulateSigTermOnQuit(done)

	var wg sync.WaitGroup

	wg.Go(func() {
		defer log.Info("backup: arpc session handler shutting down")

		base := 500 * time.Millisecond
		maxWait := 30 * time.Second
		factor := 2.0
		jitter := 0.2
		backoff := base

		var session *arpc.StreamPipe

		var currentSnap *snapshots.Snapshot
		currentReadMode := *readMode

		for {
			select {
			case <-ctx.Done():
				if session != nil {
					session.Close()
				}
				return
			default:
				log.Info("backup: attempting connection")

				var err error
				session, err = arpc.ConnectToServer(ctx, address, headers, tlsConfig)
				if err != nil {
					if strings.Contains(err.Error(), "(code 403)") {
						log.Error(err, "backup: authorization failed, shutting down")
						return
					}

					mult := 1 + jitter*(2*rand.Float64()-1)
					sleep := min(time.Duration(float64(backoff)*mult), maxWait)
					log.Warn(fmt.Sprintf("Connection failed, retrying in %v", sleep), "error", err.Error())

					select {
					case <-ctx.Done():
						return
					case <-time.After(sleep):
						backoff = min(time.Duration(float64(backoff)*factor), maxWait)
						continue
					}
				}
				// Check context AFTER connection to avoid race with cleanup
				select {
				case <-ctx.Done():
					session.Close()
					return
				default:
				}

				session.SetRouter(arpc.NewRouter())
				log.Info("arpc: connection established")
				backoff = base
			}

			if currentSnap == nil {
				snap, backupMode, err := Backup(session, *sourceMode, currentReadMode, *drive, *backupID)
				if err != nil {
					log.Error(err, "backup: initiation failed")
					cancel()
					return
				}
				fmt.Println(BACKUP_MODE_PREFIX + backupMode)
				currentSnap = snap
				currentReadMode = backupMode
			}

			fs := agentfs.NewAgentFSServer(*backupID, currentReadMode, *currentSnap)
			router := session.GetRouter()
			if router == nil {
				session.SetRouter(arpc.NewRouter())
				router = session.GetRouter()
			}

			fs.RegisterHandlers(router)
			log.Info("backup: agentfs server registered, session ready",

				"snapshot_path", currentSnap.Path, "mode", currentReadMode)

			if err := session.Serve(); err != nil {
				log.Warn("aRPC connection lost, attempting recovery", "error", err.Error())
				session.Close()
				session = nil

				select {
				case <-ctx.Done():
					return
				default:
				}
			} else {
				return
			}
		}
	})

	go func() {
		sig := <-done
		log.Info(fmt.Sprintf("backup: received signal %v", sig))
		cancel()
	}()

	wg.Wait()

	if session, ok := activeSessions.Get(*backupID); ok {
		session.Close()
	}
	log.Info("backup: finished")
	os.Exit(0)
}

func ExecBackup(sourceMode string, readMode string, drive string, backupID string) (string, int, error) {
	log.Info("backup: exec begin")
	if err := validate.ValidateJobId(backupID); err != nil {
		log.Error(err, "ExecBackup: backupID validation failed")
		return "", -1, fmt.Errorf("invalid backupID: %w", err)
	}

	tokenBytes, err := crypto.SecureRandomBytes(32)
	if err != nil {
		return "", -1, err
	}
	token := base64.StdEncoding.EncodeToString(tokenBytes)

	tokenFile := filepath.Join(os.TempDir(), fmt.Sprintf(".pbs-plus-token-backup-%s", backupID))
	if err := os.WriteFile(tokenFile, []byte(token), 0600); err != nil {
		return "", -1, err
	}

	defer func() {
		time.Sleep(5 * time.Second)
		if err := os.Remove(tokenFile); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}()

	execCmd, err := os.Executable()
	if err != nil {
		log.Error(err, "ExecBackup: os.Executable failed")
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
		"--id=" + backupID,
		"--token=" + token,
	}

	cmd := exec.Command(execCmd, args...)
	setProcAttributes(cmd)

	cmd.Env = os.Environ()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Error(err, "ExecBackup: StdoutPipe failed")
		return "", -1, err
	}

	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		log.Error(err, "ExecBackup: StderrPipe failed")
		return "", -1, err
	}

	if err := cmd.Start(); err != nil {
		log.Error(err, "ExecBackup: cmd.Start failed")
		return "", -1, err
	}
	log.Info("backup: child process started",

		"args", strings.Join(args, " "), "pid", cmd.Process.Pid)

	errScanner := bufio.NewScanner(stderrPipe)
	scanner := bufio.NewScanner(stdoutPipe)

	backupMode := make(chan string, 1)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if mode, found := strings.CutPrefix(line, BACKUP_MODE_PREFIX); found {
				log.Info("backup: detected mode", "mode", mode)
				backupMode <- mode
			} else {
				log.Info(line, "forked", true)

			}
		}
		if err := scanner.Err(); err != nil {
			log.Warn("execBackup: stdout scanner error", "error", err.Error())
		}
	}()

	go func() {
		for errScanner.Scan() {
			log.Error(errors.New(errScanner.Text()), "", "forked", true)

		}
		if err := errScanner.Err(); err != nil {
			log.Warn("execBackup: stderr scanner error", "error", err.Error())
		}
	}()

	mode := strings.TrimSpace(<-backupMode)
	log.Info("backup: returning to parent",

		"pid", cmd.Process.Pid, "mode", mode)

	return mode, cmd.Process.Pid, nil
}

func Backup(rpcSess *arpc.StreamPipe, sourceMode string, readMode string, drive string, backupID string) (*snapshots.Snapshot, string, error) {
	log.Info("backup: begin")
	if err := validate.ValidateJobId(backupID); err != nil {
		log.Error(err, "Backup: backupID validation failed")
		return nil, "", fmt.Errorf("invalid backupID: %w", err)
	}

	store, err := agent.NewBackupStore()
	if err != nil {
		log.Error(err, "Backup: NewBackupStore failed")
		return nil, "", err
	}
	if existingSession, ok := activeSessions.Get(backupID); ok {
		log.Info("backup: closing existing session")
		existingSession.Close()
		if err := store.EndBackup(backupID); err != nil {
			log.Error(err, "")
		}
	}

	if hasActive, err := store.HasActiveBackupForJob(backupID); hasActive || err != nil {
		if err != nil {
			log.Error(err, "Backup: HasActiveBackupForJob failed")
			return nil, "", err
		}
		log.Info("backup: ending previous active backup")
		if err := store.EndBackup(backupID); err != nil {
			log.Error(err, "")
		}
	}

	if err := store.StartBackup(backupID); err != nil {
		log.Error(err, "Backup: StartBackup failed")
		return nil, "", err
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
			log.Info("backup: configured direct mode",

				"drive", drive, "path", path)

		default:
			var err error
			snapshot, err = snapshots.Manager.CreateSnapshot(backupID, drive)
			if err != nil && snapshot == (snapshots.Snapshot{}) {
				log.Error(err, "Backup: VSS snapshot failed; switching to direct mode", "drive", drive)
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
				log.Info("backup: snapshot created",

					"drive", drive, "path", snapshot.Path)

			}
		}
	} else {
		snapshot = snapshots.Snapshot{
			Path:        "/",
			TimeStarted: time.Now(),
			SourcePath:  "/",
			Direct:      true,
		}
		log.Info("backup: using root snapshot")
	}

	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &backupSession{
		backupID: backupID,
		ctx:      sessionCtx,
		cancel:   cancel,
		store:    store,
		snapshot: snapshot,
	}
	activeSessions.Set(backupID, session)

	return &snapshot, backupMode, nil
}
