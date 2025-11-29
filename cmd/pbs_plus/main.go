//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc/bootstrap"
	"github.com/pbs-plus/pbs-plus/internal/backend/backup"
	"github.com/pbs-plus/pbs-plus/internal/proxy"
	"github.com/pbs-plus/pbs-plus/internal/proxy/controllers/agents"
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

	storeInstance, err := store.Initialize(mainCtx, nil)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize store").Write()
		return
	}

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

	if err = storeInstance.MigrateLegacyData(); err != nil {
		syslog.L.Error(err).WithMessage("error migrating legacy database").Write()
		return
	}

	if err := proxy.ModifyPBSHandlebars("/usr/share/javascript/proxmox-backup/index.hbs", "/usr/share/javascript/proxmox-backup/js"); err != nil {
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

	serverPublicIP := "0.0.0.0"
	tunnelPort := 8018
	networkPrefix := netip.MustParsePrefix("10.99.0.0/16")
	bootstrapStateFile := "/etc/proxmox-backup/pbs-plus/bootstrap-state.json"

	bootstrapServer, err := bootstrap.NewServerBootstrap(serverPublicIP, tunnelPort, networkPrefix, bootstrapStateFile)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to initialize bootstrap server").Write()
		return
	}

	// Build and store the aRPC node with tunnel transport
	arpcListenAddr := "10.99.0.1:8700" // Internal tunnel address for aRPC
	arpcNode, err := bootstrapServer.BuildServerNode(mainCtx, arpcListenAddr)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to build aRPC node").Write()
		return
	}
	defer arpcNode.Close()
	storeInstance.Node = arpcNode

	arpcNode.Start()

	secKeyPath := "/etc/proxmox-backup/pbs-plus/.key"

	if _, err := os.Lstat(secKeyPath); err != nil {
		key, err := utils.GenerateSecretKey(48)
		if err == nil {
			_ = os.WriteFile(secKeyPath, []byte(key), 0640)
		}
	}

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

	tunnelMux := http.NewServeMux() // For tunnel interface only
	apiMux := http.NewServeMux()    // For API interface

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

	apiMux.HandleFunc("/plus/agent/bootstrap", agents.AgentBootstrapHandler(storeInstance, bootstrapServer))
	apiMux.HandleFunc("/plus/agent/install/win", plus.AgentInstallScriptHandler(storeInstance, Version))
	apiMux.HandleFunc("/api2/json/plus/binary", plus.DownloadBinary(storeInstance, Version))
	apiMux.HandleFunc("/api2/json/plus/updater-binary", plus.DownloadUpdater(storeInstance, Version))
	apiMux.HandleFunc("/plus/metrics", plus.PrometheusMetricsHandler(storeInstance))

	// pprof routes
	apiMux.HandleFunc("/debug/pprof/", pprof.Index)
	apiMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	apiMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	apiMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	apiMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	// tunnel-only routes (agent communication after enrollment)
	tunnelMux.HandleFunc("/api2/json/plus/version", mw.AgentOrServer(storeInstance, plus.VersionHandler(storeInstance, Version)))
	tunnelMux.HandleFunc("/api2/json/plus/binary/checksum", mw.AgentOrServer(storeInstance, plus.DownloadChecksum(storeInstance, Version)))
	tunnelMux.HandleFunc("/api2/json/d2d/target/agent", mw.AgentOnly(storeInstance, targets.D2DTargetAgentHandler(storeInstance)))
	tunnelMux.HandleFunc("/api2/json/d2d/agent-log", mw.AgentOnly(storeInstance, agents.AgentLogHandler(storeInstance)))
	tunnelMux.HandleFunc("/api2/json/d2d/exclusion", mw.AgentOrServer(storeInstance, exclusions.D2DExclusionHandler(storeInstance)))

	apiServer := &http.Server{
		Addr:           ":8017",
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   5 * time.Minute,
		IdleTimeout:    5 * time.Minute,
		MaxHeaderBytes: 1 << 20, // 1MB
		Handler:        apiMux,
	}

	tunnelServerAddr := "10.99.0.1:8008"
	tunnelListener, err := storeInstance.Node.AdditionalListener(tunnelServerAddr)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create WireGuard listener").Write()
		return
	}

	tunnelServer := &http.Server{
		Handler:        tunnelMux,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   5 * time.Minute,
		IdleTimeout:    5 * time.Minute,
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	var endpointstunnel sync.WaitGroup

	go func() {
		endpointstunnel.Add(1)
		defer endpointstunnel.Done()
		proxy.WatchAndServe(apiServer, constants.CertFile, constants.KeyFile, []string{constants.CertFile, constants.KeyFile})
	}()

	endpointstunnel.Add(1)
	go func() {
		defer endpointstunnel.Done()
		syslog.L.Info().WithMessage(fmt.Sprintf("Starting WireGuard-only agent endpoint on %s", tunnelServerAddr)).Write()
		if err := tunnelServer.Serve(tunnelListener); err != nil {
			syslog.L.Error(err).WithMessage("http wireguard endpoint server failed")
		}
	}()

	endpointstunnel.Wait()
}
