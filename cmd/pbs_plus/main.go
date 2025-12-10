//go:build linux

package main

import (
	"context"
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

	"github.com/pbs-plus/pbs-plus/internal/backend/backup"
	rpcmount "github.com/pbs-plus/pbs-plus/internal/backend/rpc"
	jobrpc "github.com/pbs-plus/pbs-plus/internal/backend/rpc/job"
	"github.com/pbs-plus/pbs-plus/internal/mtls"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	"github.com/pbs-plus/pbs-plus/internal/web"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/agents"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/arpc"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/exclusions"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/jobs"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/plus"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/scripts"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/targets"
	"github.com/pbs-plus/pbs-plus/internal/web/controllers/tokens"
	mw "github.com/pbs-plus/pbs-plus/internal/web/middlewares"

	"net/http/pprof"

	// By default, it sets `GOMEMLIMIT` to 90% of cgroup's memory limit.
	// This is equivalent to `memlimit.SetGoMemLimitWithOpts(memlimit.WithLogger(slog.Default()))`
	// To disable logging, use `memlimit.SetGoMemLimitWithOpts` directly.
	_ "github.com/KimMachineGun/automemlimit"
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
	var jobsRun arrayFlags
	flag.Var(&jobsRun, "job", "Job ID/s to execute")
	retryAttempts := flag.String("retry", "", "Current attempt number")
	webRun := flag.Bool("web", false, "Job executed from Web UI")
	stop := flag.Bool("stop", false, "Stop Job ID instead of executing")
	flag.Var(&extExclusions, "skip", "Extra exclusions")
	flag.Parse()

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) > 0 && argsWithoutProg[0] == "clean-task-logs" {
		fmt.Println("WARNING: You are about to remove all junk logs recursively from:")
		fmt.Println("         /var/log/proxmox-backup/tasks")
		fmt.Println()
		fmt.Println("All log entries with the following substrings will be removed if found in any log file:")
		for _, substr := range backup.JunkSubstrings {
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

		removed, err := backup.RemoveJunkLogsRecursively("/var/log/proxmox-backup/tasks")
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

	hn, ok := os.LookupEnv("PBS_PLUS_HOSTNAME")
	if !ok {
		syslog.L.Error(fmt.Errorf("PBS_PLUS_HOSTNAME is not set.")).WithMessage("a required environment variable is not set. you may use /etc/proxmox-backup/pbs-plus/pbs-plus.env to modify environment variables").Write()
		return
	}

	if err := utils.ValidateHostname(hn); err != nil {
		syslog.L.Error(fmt.Errorf("PBS_PLUS_HOSTNAME is an invalid hostname/fqdn")).WithField("PBS_PLUS_HOSTNAME", hn).WithMessage("a required environment variable is invalid. you may use /etc/proxmox-backup/pbs-plus/pbs-plus.env to modify environment variables").Write()
		return
	}

	storeInstance, err := store.Initialize(mainCtx, nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize store").Write()
		return
	}

	// Handle single job execution
	if len(jobsRun) > 0 {
		for _, jobRun := range jobsRun {
			jobTask, err := storeInstance.Database.GetJob(jobRun)
			if err != nil {
				syslog.L.Error(err).WithField("jobId", jobRun).Write()
				return
			}

			if retryAttempts == nil || *retryAttempts == "" {
				system.RemoveAllRetrySchedules(jobTask)
			}

			arrExtExc := []string(extExclusions)

			args := &jobrpc.QueueArgs{
				Job:             jobTask,
				SkipCheck:       true,
				Stop:            *stop,
				Web:             *webRun,
				ExtraExclusions: arrExtExc,
			}
			var reply jobrpc.QueueReply

			conn, err := net.DialTimeout("unix", constants.JobMutateSocketPath, 5*time.Minute)
			if err != nil {
				syslog.L.Error(err).WithField("jobId", jobRun).Write()
				return
			} else {
				rpcClient := rpc.NewClient(conn)
				err = rpcClient.Call("JobRPCService.Queue", args, &reply)
				rpcClient.Close()
				if err != nil {
					syslog.L.Error(err).WithField("jobId", jobRun).Write()
					return
				}
				if reply.Status != 200 {
					syslog.L.Error(err).WithField("jobId", jobRun).Write()
					return
				}
			}
		}
		return
	}

	if err := web.ModifyPBSHandlebars("/usr/share/javascript/proxmox-backup/index.hbs", "/usr/share/javascript/proxmox-backup/js"); err != nil {
		syslog.L.Error(err).WithMessage("failed to mount modified proxmox-backup-gui.js").Write()
		return
	}

	// cleanup previously queued jobs
	queuedJobs, err := storeInstance.Database.GetAllQueuedJobs()
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to get all queued jobs").Write()
	}

	tx, err := storeInstance.Database.NewTransaction()
	if err == nil {
		for _, queuedJob := range queuedJobs {
			task, err := proxmox.GenerateTaskErrorFile(queuedJob, fmt.Errorf("server was restarted before job started during queue"), nil)
			if err != nil {
				continue
			}

			queueTaskPath, err := proxmox.GetLogPath(queuedJob.LastRunUpid)
			if err == nil {
				os.Remove(queueTaskPath)
			}

			queuedJob.LastRunUpid = task.UPID
			err = storeInstance.Database.UpdateJob(tx, queuedJob)
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

	serverConfig, err := storeInstance.GetServerTLSConfig()
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

	backupManager := backup.NewManager(mainCtx)

	go func() {
		for {
			select {
			case <-mainCtx.Done():
				syslog.L.Error(mainCtx.Err()).WithMessage("job rpc server cancelled")
				return
			default:
				if err := jobrpc.RunJobRPCServer(mainCtx, constants.JobMutateSocketPath, backupManager, storeInstance); err != nil {
					syslog.L.Error(err).WithMessage("job rpc server failed, restarting")
				}
			}
		}
	}()

	agentMux := http.NewServeMux()
	apiMux := http.NewServeMux()

	// API routes
	apiMux.HandleFunc("/api2/json/d2d/backup", mw.ServerOnly(storeInstance, jobs.D2DJobHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/target", mw.ServerOnly(storeInstance, targets.D2DTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/script", mw.ServerOnly(storeInstance, scripts.D2DScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/token", mw.ServerOnly(storeInstance, tokens.D2DTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOrServer(storeInstance, exclusions.D2DExclusionHandler(storeInstance)))

	// ExtJS routes with path parameters
	apiMux.HandleFunc("/api2/extjs/d2d/backup", mw.ServerOnly(storeInstance, jobs.ExtJsJobRunHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", mw.ServerOnly(storeInstance, targets.ExtJsTargetHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.ServerOnly(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}/s3-secret", mw.ServerOnly(storeInstance, targets.ExtJsTargetS3SecretHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-mount/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsMountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsUnmountHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-unmount-all/{datastore}", mw.ServerOnly(storeInstance, jobs.ExtJsUnmountAllHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", mw.ServerOnly(storeInstance, scripts.ExtJsScriptHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", mw.ServerOnly(storeInstance, scripts.ExtJsScriptSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", mw.ServerOnly(storeInstance, tokens.ExtJsTokenHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", mw.ServerOnly(storeInstance, tokens.ExtJsTokenSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.ServerOnly(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.ServerOnly(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job", mw.ServerOnly(storeInstance, jobs.ExtJsJobHandler(storeInstance)))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", mw.ServerOnly(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance)))

	// Agent routes
	agentMux.HandleFunc("/api2/json/plus/version", mw.AgentOrServer(storeInstance, plus.VersionHandler(storeInstance, Version)))
	agentMux.HandleFunc("/api2/json/plus/binary", plus.DownloadBinary(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/updater-binary", plus.DownloadUpdater(storeInstance, Version))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", mw.AgentOrServer(storeInstance, plus.DownloadChecksum(storeInstance, Version)))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, targets.D2DTargetAgentHandler(storeInstance)))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, agents.AgentLogHandler(storeInstance)))

	// aRPC route
	agentMux.HandleFunc("/plus/arpc", mw.AgentOnly(storeInstance, arpc.ARPCHandler(storeInstance)))

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
		Addr:           ":8017",
		Handler:        apiMux,
		ReadTimeout:    constants.HTTPReadTimeout,
		WriteTimeout:   constants.HTTPWriteTimeout,
		IdleTimeout:    constants.HTTPIdleTimeout,
		MaxHeaderBytes: constants.HTTPMaxHeaderBytes,
	}

	agentServer := &http.Server{
		Addr:           ":8008",
		Handler:        agentMux,
		TLSConfig:      serverConfig,
		ReadTimeout:    constants.HTTPReadTimeout,
		WriteTimeout:   constants.HTTPWriteTimeout,
		IdleTimeout:    constants.HTTPIdleTimeout,
		MaxHeaderBytes: constants.HTTPMaxHeaderBytes,
	}

	var endpointsWg sync.WaitGroup

	endpointsWg.Add(1)
	go web.WatchAndServe(apiServer, constants.CertFile, constants.KeyFile, []string{constants.CertFile, constants.KeyFile}, &endpointsWg)

	endpointsWg.Add(1)
	go func() {
		defer endpointsWg.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting agent endpoint on %s", apiServer.Addr)).Write()
		if err := storeInstance.ListenAndServeAgentEndpoint(agentServer); err != nil {
			syslog.L.Error(err).WithMessage("http agent endpoint server failed")
		}
	}()

	endpointsWg.Wait()
}
