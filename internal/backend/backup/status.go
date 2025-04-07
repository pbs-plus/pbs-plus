//go:build linux

package backup

import (
	"fmt"
	"net"
	"net/rpc"
	"time"

	rpcmount "github.com/pbs-plus/pbs-plus/internal/proxy/rpc"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/proxmox"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

func updateJobStatus(succeeded bool, warningsNum int, job types.Job, task proxmox.Task, storeInstance *store.Store) error {
	// Update task status
	taskFound, err := proxmox.Session.GetTaskByUPID(task.UPID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get task by upid").Write()
		return err
	}

	// Update job status
	latestJob, err := storeInstance.Database.GetJob(job.ID)
	if err != nil {
		syslog.L.Error(err).WithMessage("unable to get job").Write()
		return err
	}

	latestJob.CurrentPID = job.CurrentPID
	latestJob.LastRunUpid = taskFound.UPID
	latestJob.LastRunState = taskFound.Status
	latestJob.LastRunEndtime = taskFound.EndTime

	if warningsNum > 0 && succeeded {
		latestJob.LastRunState = fmt.Sprintf("WARNINGS: %d", warningsNum)
	}

	if succeeded {
		latestJob.LastSuccessfulUpid = taskFound.UPID
		latestJob.LastSuccessfulEndtime = task.EndTime
	}

	args := &rpcmount.UpdateArgs{
		Job: latestJob,
	}
	var reply rpcmount.UpdateReply

	conn, err := net.DialTimeout("unix", constants.JobMutateSocketPath, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed to dial RPC server: %w", err)
	}

	rpcClient := rpc.NewClient(conn)
	err = rpcClient.Call("JobRPCService.Update", args, &reply)
	rpcClient.Close()
	if err != nil {
		return fmt.Errorf("failed to call backup RPC: %w", err)
	}
	if reply.Status != 200 {
		return fmt.Errorf("backup RPC returned an error %d: %s", reply.Status, reply.Message)
	}

	return nil
}
