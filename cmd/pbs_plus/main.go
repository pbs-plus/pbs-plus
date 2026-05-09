//go:build linux

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	backend "github.com/pbs-plus/pbs-plus/internal/backend"
	"github.com/pbs-plus/pbs-plus/internal/backend/jobs/backup"
	"github.com/pbs-plus/pbs-plus/internal/backend/rpc/job"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/web"

	_ "github.com/pbs-plus/pbs-plus/internal/memlimit"
)

var Version = "v0.0.0"

func init() {
	conf.IsServer = true
	conf.InitBuffers()
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
	_ = flag.String("retry", "", "Current attempt number") // legacy flag, no-op
	webRun := flag.Bool("web", false, "Backup executed from Web UI")
	stop := flag.Bool("stop", false, "Stop Job ID instead of executing")
	flag.Var(&extExclusions, "skip", "Extra exclusions")
	flag.Parse()

	argsWithoutProg := os.Args[1:]

	if len(argsWithoutProg) > 0 && argsWithoutProg[0] == "clean-task-logs" {
		runCleanTaskLogs()
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

	if len(backupsRun) > 0 || len(restoresRun) > 0 {
		runOneShotJobs(storeInstance, backupsRun, restoresRun, extExclusions, *stop, *webRun)
		return
	}

	if err := validateEnvironment(); err != nil {
		syslog.L.Error(err).Write()
		return
	}

	if err := web.ModifyPBSHandlebars(
		"/usr/share/javascript/proxmox-backup/index.hbs",
		"/usr/share/javascript/proxmox-backup/js",
	); err != nil {
		syslog.L.Error(err).WithMessage("failed to mount modified proxmox-backup-gui.js").Write()
		return
	}

	// Bootstrap: cert generation, secret key, token manager, mount cleanup,
	// queue cleanup, scheduler, and RPC servers.
	_, _, err = backend.Bootstrap(mainCtx, storeInstance)
	if err != nil {
		syslog.L.Error(err).WithMessage("bootstrap failed").Write()
		return
	}

	// Create and start all HTTP/ARPC servers.
	server, err := web.NewServer(storeInstance, Version)
	if err != nil {
		syslog.L.Error(err).WithMessage("failed to create server").Write()
		return
	}

	server.StartAll()

	// Wait for shutdown signal.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	syslog.L.Info().WithMessage(fmt.Sprintf("received %s, shutting down gracefully", sig)).Write()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		syslog.L.Error(err).WithMessage("shutdown error").Write()
	}

	mainCancel()
	syslog.L.Info().WithMessage("shutdown complete").Write()
}

func validateEnvironment() error {
	if err := proxmox.CleanupPbsPlusActiveTasks(); err != nil {
		return fmt.Errorf("CleanupPbsPlusActiveTasks: %w", err)
	}

	hn, ok := conf.Env.Hostname, conf.Env.Hostname != ""
	if !ok {
		return fmt.Errorf("PBS_PLUS_HOSTNAME is not set; you may use /etc/proxmox-backup/pbs-plus/pbs-plus.env to modify environment variables")
	}

	if err := types.ValidateHostname(hn); err != nil {
		return fmt.Errorf("PBS_PLUS_HOSTNAME is an invalid hostname/fqdn: %s", hn)
	}

	return nil
}

func runOneShotJobs(storeInstance *store.Store, backupsRun, restoresRun, extExclusions []string, stop, webRun bool) {
	conn, err := net.DialTimeout("unix", conf.JobMutateSocketPath, 5*time.Minute)
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

		args := &job.BackupQueueArgs{
			Job:             backupTask,
			SkipCheck:       true,
			Stop:            stop,
			Web:             webRun,
			ExtraExclusions: extExclusions,
		}
		var reply job.QueueReply
		if err := rpcClient.Call("JobRPCService.BackupQueue", args, &reply); err != nil {
			syslog.L.Error(err).WithField("backupId", backupRun).Write()
			continue
		}
		if reply.Status != 200 {
			syslog.L.Error(fmt.Errorf("%s", reply.Message)).WithField("backupId", backupRun).Write()
		}
	}

	for _, restoreRun := range restoresRun {
		restoreTask, err := storeInstance.Database.GetRestore(restoreRun)
		if err != nil {
			syslog.L.Error(err).WithField("restoreId", restoreRun).Write()
			continue
		}

		args := &job.RestoreQueueArgs{
			Job:       restoreTask,
			SkipCheck: true,
			Stop:      stop,
			Web:       webRun,
		}
		var reply job.QueueReply
		if err := rpcClient.Call("JobRPCService.RestoreQueue", args, &reply); err != nil {
			syslog.L.Error(err).WithField("restoreId", restoreRun).Write()
			continue
		}
		if reply.Status != 200 {
			syslog.L.Error(fmt.Errorf("%s", reply.Message)).WithField("restoreId", restoreRun).Write()
		}
	}
}

func runCleanTaskLogs() {
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
}
