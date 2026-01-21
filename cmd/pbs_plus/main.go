//go:build linux

package main

import (
	"context"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/backend/helpers"
	backend "github.com/pbs-plus/pbs-plus/internal/backend/jobs"
	rpcmount "github.com/pbs-plus/pbs-plus/internal/backend/rpc"
	backuprpc "github.com/pbs-plus/pbs-plus/internal/backend/rpc/job"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/tasks"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/agents"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/exclusions"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/jobs"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/plus"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/scripts"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/targets"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/tokens"
	mw "github.com/pbs-plus/pbs-plus/internal/web/middlewares"

	"net/http/pprof"

	_ "github.com/pbs-plus/pbs-plus/internal/utils/memlimit"
)

var Version = "v0.0.0"

func init() {
	utils.IsServer = true
}

type arrayFlags []string

func (i *arrayFlags) String() string {
	return fmt.Sprintf("%v", *i)
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	var extExclusions arrayFlags
	var backupsRun arrayFlags
	var restoresRun arrayFlags
	flag.Var(&backupsRun, "backup-job", "Backup ID/s to execute")
	flag.Var(&restoresRun, "restore-job", "Restore ID/s to execute")
	retryAttempts := flag.String("retry", "", "Current attempt number")
	webRun := flag.Bool("web", false, "Backup executed from Web UI")
	stop := flag.Bool("stop", false, "Stop Job ID instead of executing")
	flag.Var(&extExclusions, "skip", "Extra exclusions")
	flag.Parse()

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) > 0 && argsWithoutProg[0] == "clean-task-logs" {
		fmt.Println("WARNING: You are about to remove all junk logs recursively from:")
		fmt.Println("         /var/log/proxmox-backup/tasks")
		fmt.Println()
		fmt.Println("All log entries with the following substrings will be removed if found in any log file:")
		for _, substr := range helpers.JunkSubstrings {
			fmt.Printf(" - %s\n", substr)
		}
		fmt.Println()
		fmt.Println("If this is not what you intend, press Ctrl+C within the next 10 seconds to cancel.")
		fmt.Println()

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt)

		cancelChan := make(chan struct{})
		go func() {
			<-sigChan
			fmt.Println("\nOperation cancelled by user.")
			close(cancelChan)
		}()

		for i := 10; i > 0; i-- {
			select {
			case <-cancelChan:
				// User cancelled the operation.
				return
			default:
				fmt.Printf("Proceeding in %d seconds...\n", i)
				time.Sleep(1 * time.Second)
			}
		}

		fmt.Println("Proceeding with log cleanup...")

		removed, err := helpers.RemoveJunkLogsRecursively("/var/log/proxmox-backup/tasks")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Successfully removed %d of junk lines from all task logs files.\n", removed)
		return
	}

	syslog.L.Server = true
	if err := syslog.L.SetServiceLogger(); err != nil {
		syslog.L.Error(err).Write()
	}
	_ = proxmox.GetToken()

	storeInstance, err := store.Initialize(mainCtx, nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize store").Write()
		return
	}

	// Handle backup execution
	if len(backupsRun) > 0 || len(restoresRun) > 0 {
		conn, err := net.DialTimeout("unix", constants.JobMutateSocketPath, 5*time.Minute)
		if err != nil {
			syslog.L.Error(err).
				WithField("backups", backupsRun).
				WithField("restores", restoresRun).
				Write()
			return
		}
		rpcClient := rpc.NewClient(conn)
		defer rpcClient.Close()

		for _, backupRun := range backupsRun {
			backupTask, err := storeInstance.Database.GetBackup(backupRun)
			if err != nil {
				syslog.L.Error(err).WithField("backupId", backupRun).Write()
				continue
			}

			if retryAttempts == nil || *retryAttempts == "" {
				backupTask.RemoveAllRetrySchedules(context.Background())
			}

			arrExtExc := []string(extExclusions)

			args := &backuprpc.BackupQueueArgs{
				Job:             backupTask,
				SkipCheck:       true,
				Stop:            *stop,
				Web:             *webRun,
				ExtraExclusions: arrExtExc,
			}
			var reply backuprpc.QueueReply

			err = rpcClient.Call("JobRPCService.BackupQueue", args, &reply)
			if err != nil {
				syslog.L.Error(err).WithField("backupId", backupRun).Write()
				continue
			}
			if reply.Status != 200 {
				syslog.L.Error(err).WithField("backupId", backupRun).Write()
				continue
			}
		}

		for _, restoreRun := range restoresRun {
			restoreTask, err := storeInstance.Database.GetRestore(restoreRun)
			if err != nil {
				syslog.L.Error(err).WithField("restoreId", restoreRun).Write()
				continue
			}

			args := &backuprpc.RestoreQueueArgs{
				Job:       restoreTask,
				SkipCheck: true,
				Stop:      *stop,
				Web:       *webRun,
			}
			var reply backuprpc.QueueReply

			err = rpcClient.Call("JobRPCService.RestoreQueue", args, &reply)
			if err != nil {
				syslog.L.Error(err).WithField("restoreId", restoreRun).Write()
				continue
			}
			if reply.Status != 200 {
				syslog.L.Error(err).WithField("restoreId", restoreRun).Write()
				continue
			}
		}

		return
	}

	proxmox.CleanupPbsPlusActiveTasks()

	hn, ok := os.LookupEnv("PBS_PLUS_HOSTNAME")
	if !ok {
		syslog.L.Error(fmt.Errorf("PBS_PLUS_HOSTNAME is not set.")).WithMessage("a required environment variable is not set. you may use /etc/proxmox-backup/pbs-plus/pbs-plus.env to modify environment variables").Write()
		return
	}

	if err := utils.ValidateHostname(hn); err != nil {
		syslog.L.Error(fmt.Errorf("PBS_PLUS_HOSTNAME is an invalid hostname/fqdn")).WithField("PBS_PLUS_HOSTNAME", hn).WithMessage("a required environment variable is invalid. you may use /etc/proxmox-backup/pbs-plus/pbs-plus.env to modify environment variables").Write()
		return
	}

	if err := web.ModifyPBSHandlebars("/usr/share/javascript/proxmox-backup/index.hbs", "/usr/share/javascript/proxmox-backup/js"); err != nil {
		syslog.L.Error(err).WithMessage("failed to mount modified proxmox-backup-gui.js").Write()
		return
	}

	// cleanup previously queued backups
	queuedBackups, err := storeInstance.Database.GetAllQueuedBackups()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get all queued backups").Write()
	}

	tx, err := storeInstance.Database.NewTransaction()
	if err == nil {
		for _, queuedBackup := range queuedBackups {
			task, err := tasks.GenerateBackupTaskErrorFile(queuedBackup, fmt.Errorf("server was restarted before backup started during queue"), nil)
			if err != nil {
				continue
			}

			queueTaskPath, err := proxmox.GetLogPath(queuedBackup.History.LastRunUpid)
			if err == nil {
				os.Remove(queueTaskPath)
			}

			queuedBackup.History.LastRunUpid = task.UPID
			err = storeInstance.Database.UpdateBackup(tx, queuedBackup)
			if err != nil {
				continue
			}
		}
		tx.Commit()
	}

	secKeyPath := "/etc/proxmox-backup/pbs-plus/.key"

	if _, err := os.Lstat(secKeyPath); err != nil {
		key, err := utils.GenerateSecretKey(48)
		if err == nil {
			_ = os.WriteFile(secKeyPath, []byte(key), 0640)
		}
	}

	secKey, err := os.ReadFile(secKeyPath)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to read .key").Write()
		return
	}

	err = storeInstance.ValidateServerCertificates()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to generate local CA and server cert").Write()
		return
	}

	serverConfig, err := storeInstance.GetAPIServerTLSConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to build server TLS config").Write()
		return
	}

	// Initialize token manager
	tokenManager, err := mtls.NewTokenManager(mtls.TokenConfig{
		TokenExpiration: constants.AuthTokenExpiration,
		SecretKey:       string(secKey),
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize token manager").Write()
		return
	}
	storeInstance.Database.TokenManager = tokenManager

	// Unmount and remove all stale mount points
	// Get all mount points under the base path
	mountPoints, err := filepath.Glob(filepath.Join(constants.AgentMountBasePath, "*"))
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to find agent mount base path").Write()
	}

	// Unmount each one
	for _, mountPoint := range mountPoints {
		umount := exec.Command("umount", "-lf", mountPoint)
		umount.Env = os.Environ()
		if err := umount.Run(); err != nil {
			// Optionally handle individual unmount errors
			syslog.L.Error(err).WithMessage("failed to unmount some mounted agents").Write()
		}
	}

	if err := os.RemoveAll(constants.AgentMountBasePath); err != nil {
		syslog.L.Error(err).WithMessage("failed to remove directory").Write()
	}

	if err := os.Mkdir(constants.AgentMountBasePath, 0700); err != nil {
		syslog.L.Error(err).WithMessage("failed to recreate directory").Write()
	}

	go func() {
		for {
			select {
			case <-mainCtx.Done():
				syslog.L.Error(mainCtx.Err()).WithMessage("mount rpc server cancelled")
				return
			default:
				if err := rpcmount.RunRPCServer(mainCtx, constants.MountSocketPath, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("mount rpc server failed, restarting")
				}
			}
		}
	}()

	manager := backend.NewManager(mainCtx, utils.MaxConcurrentClients, 100, true)

	go func() {
		for {
			select {
			case <-mainCtx.Done():
				syslog.L.Error(mainCtx.Err()).WithMessage("backup rpc server cancelled")
				return
			default:
				if err := backuprpc.RunJobRPCServer(mainCtx, constants.JobMutateSocketPath, manager, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("backup rpc server failed, restarting")
				}
			}
		}
	}()

	agentMux := http.NewServeMux()
	apiMux := http.NewServeMux()

	// API routes
	apiMux.HandleFunc("/api2/json/d2d/backup", mw.ServerOnly(storeInstance, jobs.D2DBackupHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/restore", mw.ServerOnly(storeInstance, jobs.D2DRestoreHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/target", mw.ServerOnly(storeInstance, targets.D2DTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/script", mw.ServerOnly(storeInstance, scripts.D2DScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/token", mw.ServerOnly(storeInstance, tokens.D2DTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/filetree/{target}", mw.ServerOnly(storeInstance, jobs.D2DRestoreFileTree(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOrServer(storeInstance, exclusions.D2DExclusionHandler(storeInstance)))

	// ExtJS routes with path parameters
	apiMux.HandleFunc("/api2/extjs/d2d/backup", mw.ServerOnly(storeInstance, jobs.ExtJsBackupRunHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/d2d/restore", mw.ServerOnly(storeInstance, jobs.ExtJsRestoreRunHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", mw.ServerOnly(storeInstance, targets.ExtJsTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.ServerOnly(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}/s3-secret", mw.ServerOnly(storeInstance, targets.ExtJsTargetS3SecretHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-agent/{agent}", mw.ServerOnly(storeInstance, targets.ExtJsAgentSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsMountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsUnmountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount-all/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsUnmountAllHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", mw.ServerOnly(storeInstance, scripts.ExtJsScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", mw.ServerOnly(storeInstance, scripts.ExtJsScriptSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", mw.ServerOnly(storeInstance, tokens.ExtJsTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", mw.ServerOnly(storeInstance, tokens.ExtJsTokenSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.ServerOnly(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.ServerOnly(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup", mw.ServerOnly(storeInstance, jobs.ExtJsBackupHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}", mw.ServerOnly(storeInstance, jobs.ExtJsBackupSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup/{backup}/upids", mw.ServerOnly(storeInstance, jobs.ExtJsBackupUPIDsHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore", mw.ServerOnly(storeInstance, jobs.ExtJsRestoreHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-restore/{restore}", mw.ServerOnly(storeInstance, jobs.ExtJsRestoreSingleHandler(storeInstance)))

	// Agent routes
	agentMux.HandleFunc("/api2/json/plus/version", plus.VersionHandler(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/msi", plus.DownloadMsi(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/binary", plus.DownloadBinary(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/binary/sig", plus.DownloadSig(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", plus.DownloadChecksum(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, targets.D2DTargetAgentHandler(storeInstance)))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, agents.AgentLogHandler(storeInstance)))

	// Agent auth routes
	agentMux.HandleFunc("/plus/agent/bootstrap", agents.AgentBootstrapHandler(storeInstance))
	agentMux.HandleFunc("/plus/agent/renew", mw.AgentOnly(storeInstance, agents.AgentRenewHandler(storeInstance)))
	agentMux.HandleFunc("/plus/agent/install/win", plus.AgentInstallScriptHandler(storeInstance, Version))
	agentMux.HandleFunc("/plus/metrics", plus.PrometheusMetricsHandler(storeInstance))

	// pprof routes
	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	apiServer := &http.Server{
		Addr:           constants.ServerAPIExtPort,
		Handler:        apiMux,
		ReadTimeout:    constants.HTTPReadTimeout,
		WriteTimeout:   constants.HTTPWriteTimeout,
		IdleTimeout:    constants.HTTPIdleTimeout,
		MaxHeaderBytes: constants.HTTPMaxHeaderBytes,
	}

	agentServer := &http.Server{
		Addr:           constants.AgentAPIPort,
		Handler:        agentMux,
		TLSConfig:      serverConfig,
		ReadTimeout:    constants.HTTPReadTimeout,
		WriteTimeout:   constants.HTTPWriteTimeout,
		IdleTimeout:    constants.HTTPIdleTimeout,
		MaxHeaderBytes: constants.HTTPMaxHeaderBytes,
	}

	var endpointsWg sync.WaitGroup

	endpointsWg.Go(func() {
		web.WatchAndServe(apiServer, constants.CertFile, constants.KeyFile, []string{constants.CertFile, constants.KeyFile})
	})

	endpointsWg.Go(func() {
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting agent endpoint on %s", agentServer.Addr)).Write()
		if err := storeInstance.ListenAndServeAgentEndpoint(agentServer); err != nil {
			syslog.L.Error(err).WithMessage("http agent endpoint server failed")
		}
	})

	endpointsWg.Go(func() {
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting aRPC endpoint on TCP %s", constants.ARPCServerPort)).Write()

		router := arpc.NewRouter()
		router.Handle("echo", func(req *arpc.Request) (arpc.Response, error) {
			var msg string
			if err := cbor.Unmarshal(req.Payload, &msg); err != nil {
				return arpc.Response{}, arpc.WrapError(err)
			}
			data, err := cbor.Marshal(msg)
			if err != nil {
				return arpc.Response{}, arpc.WrapError(err)
			}
			return arpc.Response{Status: 200, Data: data}, nil
		})

		arpcTlsConfig, err := storeInstance.GetARPCServerTLSConfig()
		if err != nil {
			syslog.L.Error(err).WithMessage("failed to build server TLS config").Write()
			return
		}

		storeInstance.ARPCAgentsManager.SetExtraExpectFunc(func(id string, certs []*x509.Certificate) bool {
			syslog.L.Info().WithMessage("checking client authorization").WithField("id", id).Write()

			if len(certs) == 0 {
				syslog.L.Error(fmt.Errorf("no client certificates received")).WithMessage("client unauthorized").WithField("id", id).Write()
				return false
			}

			trustedCert, err := storeInstance.Database.LoadAgentHostCert(id)
			if err != nil {
				syslog.L.Error(err).WithMessage("client unauthorized").WithField("id", id).Write()
				return false
			}

			found := false
			for _, cert := range certs {
				if cert.Equal(trustedCert) {
					found = true
					break
				}
			}

			if !found {
				syslog.L.Error(fmt.Errorf("did not match trusted certificate")).WithMessage("client unauthorized").WithField("id", id).Write()
				return false
			}

			syslog.L.Info().WithMessage("client authorized").WithField("id", id).Write()

			return true
		})

		if err := arpc.ListenAndServe(storeInstance.Ctx, constants.ARPCServerPort, storeInstance.ARPCAgentsManager, arpcTlsConfig, router); err != nil {
			syslog.L.Error(err).WithMessage("arpc agent endpoint server failed")
		}
	})

	endpointsWg.Wait()
}
