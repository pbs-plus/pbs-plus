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

	"github.com/pbs-plus/pbs-plus/internal/auth/certificates"
	"github.com/pbs-plus/pbs-plus/internal/auth/server"
	"github.com/pbs-plus/pbs-plus/internal/auth/token"
	"github.com/pbs-plus/pbs-plus/internal/backend/backup"
	"github.com/pbs-plus/pbs-plus/internal/proxy"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/agents"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/arpc"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/exclusions"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/jobs"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/plus"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/scripts"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/targets"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/tokens"
	mw "github.com/pbs-plus/pbs-plus/internal/proxy/middlewares"
	rpcmount "github.com/pbs-plus/pbs-plus/internal/proxy/rpc"
	jobrpc "github.com/pbs-plus/pbs-plus/internal/proxy/rpc/job"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/system"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"

	"net/http/pprof"

	// By default, it sets `GOMEMLIMIT` to 90% of cgroup's memory limit.
	// This is equivalent to `memlimit.SetGoMemLimitWithOpts(memlimit.WithLogger(slog.Default()))`
	// To disable logging, use `memlimit.SetGoMemLimitWithOpts` directly.
	_ "github.com/KimMachineGun/automemlimit"
)

var Version = "v0.0.0"

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
	jobRun := flag.String("job", "", "Job ID to execute")
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

	storeInstance, err := store.Initialize(mainCtx, nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize store").Write()
		return
	}

	// Handle single job execution
	if *jobRun != "" {
		jobTask, err := storeInstance.Database.GetJob(*jobRun)
		if err != nil {
			syslog.L.Error(err).WithField("jobId", *jobRun).Write()
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
			syslog.L.Error(err).WithField("jobId", *jobRun).Write()
			return
		} else {
			rpcClient := rpc.NewClient(conn)
			err = rpcClient.Call("JobRPCService.Queue", args, &reply)
			rpcClient.Close()
			if err != nil {
				syslog.L.Error(err).WithField("jobId", *jobRun).Write()
				return
			}
			if reply.Status != 200 {
				syslog.L.Error(err).WithField("jobId", *jobRun).Write()
				return
			}
		}

		return
	}

	if err = storeInstance.MigrateLegacyData(); err != nil {
		syslog.L.Error(err).WithMessage("error migrating legacy database").Write()
		return
	}

	if err := proxy.ModifyPBSJavascript(); err != nil {
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

	certOpts := certificates.DefaultOptions()
	generator, err := certificates.NewGenerator(certOpts)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize certificate generator").Write()
		return
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

	serverConfig := server.DefaultConfig()
	serverConfig.CertFile = filepath.Join(certOpts.OutputDir, "server.crt")
	serverConfig.KeyFile = filepath.Join(certOpts.OutputDir, "server.key")
	serverConfig.CAFile = filepath.Join(certOpts.OutputDir, "ca.crt")
	serverConfig.CAKey = filepath.Join(certOpts.OutputDir, "ca.key")
	serverConfig.TokenSecret = string(secKey)

	if err := generator.ValidateExistingCerts(); err != nil {
		if err := generator.GenerateCA(); err != nil {
			syslog.L.Error(err).WithMessage("failed to generate certificate").Write()
			return
		}

		if err := generator.GenerateCert("server"); err != nil {
			syslog.L.Error(err).WithMessage("failed to generate certificate").Write()
			return
		}
	}

	if err := serverConfig.Validate(); err != nil {
		syslog.L.Error(err).WithMessage("failed to validate server config").Write()
		return
	}

	storeInstance.CertGenerator = generator

	err = os.Chown(serverConfig.KeyFile, 0, 34)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to change cert key permissions").Write()
		return
	}

	err = os.Chown(serverConfig.CertFile, 0, 34)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to change cert permissions").Write()
		return
	}

	proxy := exec.Command("/usr/bin/systemctl", "restart", "proxmox-backup-proxy")
	proxy.Env = os.Environ()
	_ = proxy.Run()

	// Initialize token manager
	tokenManager, err := token.NewManager(token.Config{
		TokenExpiration: serverConfig.TokenExpiration,
		SecretKey:       serverConfig.TokenSecret,
	})
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize token manager").Write()
		return
	}
	storeInstance.Database.TokenManager = tokenManager

	// Setup HTTP server
	tlsConfig, err := serverConfig.LoadTLSConfig()
	if err != nil {
		return
	}

	caRenewalCtx, cancelRenewal := context.WithCancel(context.Background())
	defer cancelRenewal()
	go func() {
		for {
			select {
			case <-caRenewalCtx.Done():
				return
			case <-time.After(time.Hour):
				if err := generator.ValidateExistingCerts(); err != nil {
					if err := generator.GenerateCA(); err != nil {
						syslog.L.Error(err).WithMessage("failed to generate CA").Write()
					}

					if err := generator.GenerateCert("server"); err != nil {
						syslog.L.Error(err).WithMessage("failed to generate server certificate").Write()
					}
				}

			}
		}
	}()

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

	backupManager := backup.NewManager(mainCtx, 512, 64)

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
	apiMux.HandleFunc("/api2/json/d2d/backup", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.D2DJobHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/target", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.D2DTargetHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/script", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, scripts.D2DScriptHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.D2DTokenHandler(storeInstance))))
	apiMux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, exclusions.D2DExclusionHandler(storeInstance))))

	// ExtJS routes with path parameters
	apiMux.HandleFunc("/api2/extjs/d2d/backup/{job}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobRunHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-target/{target}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, targets.ExtJsTargetSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, scripts.ExtJsScriptHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-script/{path}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, scripts.ExtJsScriptSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.ExtJsTokenHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-token/{token}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, tokens.ExtJsTokenSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/d2d-exclusion/{exclusion}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, exclusions.ExtJsExclusionSingleHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobHandler(storeInstance))))
	apiMux.HandleFunc("/api2/extjs/config/disk-backup-job/{job}", mw.ServerOnly(storeInstance, mw.CORS(storeInstance, jobs.ExtJsJobSingleHandler(storeInstance))))

	// Agent routes
	agentMux.HandleFunc("/api2/json/plus/version", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.VersionHandler(storeInstance, Version))))
	agentMux.HandleFunc("/api2/json/plus/binary", mw.CORS(storeInstance, plus.DownloadBinary(storeInstance, Version)))
	agentMux.HandleFunc("/api2/json/plus/updater-binary", mw.CORS(storeInstance, plus.DownloadUpdater(storeInstance, Version)))
	agentMux.HandleFunc("/api2/json/plus/binary/checksum", mw.AgentOrServer(storeInstance, mw.CORS(storeInstance, plus.DownloadChecksum(storeInstance, Version))))
	agentMux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, targets.D2DTargetAgentHandler(storeInstance))))
	agentMux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentLogHandler(storeInstance))))

	// aRPC route
	agentMux.HandleFunc("/plus/arpc", mw.AgentOnly(storeInstance, arpc.ARPCHandler(storeInstance)))

	// Agent auth routes
	agentMux.HandleFunc("/plus/agent/bootstrap", mw.CORS(storeInstance, agents.AgentBootstrapHandler(storeInstance)))
	agentMux.HandleFunc("/plus/agent/renew", mw.AgentOnly(storeInstance, mw.CORS(storeInstance, agents.AgentRenewHandler(storeInstance))))
	agentMux.HandleFunc("/plus/agent/install/win", mw.CORS(storeInstance, plus.AgentInstallScriptHandler(storeInstance, Version)))

	// pprof routes
	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	apiServer := &http.Server{
		Addr:           serverConfig.Address,
		Handler:        apiMux,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		IdleTimeout:    serverConfig.IdleTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	agentServer := &http.Server{
		Addr:           serverConfig.AgentAddress,
		Handler:        agentMux,
		TLSConfig:      tlsConfig,
		ReadTimeout:    serverConfig.ReadTimeout,
		WriteTimeout:   serverConfig.WriteTimeout,
		IdleTimeout:    serverConfig.IdleTimeout,
		MaxHeaderBytes: serverConfig.MaxHeaderBytes,
	}

	var endpointsWg sync.WaitGroup

	endpointsWg.Add(1)
	go func() {
		defer endpointsWg.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("starting api endpoint on %s", serverConfig.Address)).Write()
		if err := apiServer.ListenAndServeTLS(server.ProxyCert, server.ProxyKey); err != nil {
			syslog.L.Error(err).WithMessage("http api server failed")
		}
	}()

	endpointsWg.Add(1)
	go func() {
		defer endpointsWg.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("starting agent endpoint on %s", serverConfig.AgentAddress)).Write()
		if err := agentServer.ListenAndServeTLS(serverConfig.CertFile, serverConfig.KeyFile); err != nil {
			syslog.L.Error(err).WithMessage("http agent endpoint server failed")
		}
	}()

	endpointsWg.Wait()
}
