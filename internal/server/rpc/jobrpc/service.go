//go:build linux

package jobrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/backup"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/mtf"
	"github.com/pbs-plus/pbs-plus/internal/server/restore"
	"github.com/pbs-plus/pbs-plus/internal/server/rpcserver"
)

const ServiceName = "JobRPCService"

type BackupQueueArgs struct {
	Job             coredb.Backup
	SkipCheck       bool
	Web             bool
	Stop            bool
	ExtraExclusions []string
}

type RestoreQueueArgs struct {
	Job       coredb.Restore
	SkipCheck bool
	Web       bool
	Stop      bool
}

type MtfJobQueueArgs struct {
	JobID string
	Web   bool
	Stop  bool
}

type QueueReply struct {
	Status      int
	Message     string
	ExecutionID string
	UPID        string
}

type Service struct {
	ctx    context.Context
	Store  *application.Runtime
	Engine *jobs.Engine
}

// blockedReason reports why a user-triggered run cannot start now, or "" when it can; resourceKey is empty for backups because concurrent reads of one target are safe, while restores pass their destination because they write it.
func (s *Service) blockedReason(kind, definitionID, resourceKey string) string {
	if _, err := s.Engine.ActiveExecution(s.ctx, kind, definitionID); err == nil {
		return "this job is already queued or running"
	}
	if resourceKey == "" {
		return ""
	}
	holder, err := s.Engine.ResourceHolder(s.ctx, resourceKey)
	if err != nil || holder.DefinitionID == definitionID {
		return ""
	}
	return fmt.Sprintf("%q is in use by %s job %q", resourceKey, holder.Kind, holder.DefinitionID)
}

func (s *Service) BackupQueue(args *BackupQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if err := backup.CancelQueued(s.Store, args.Job); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
		return nil
	}

	if reason := s.blockedReason(jobs.WorkflowBackup, args.Job.ID, ""); reason != "" {
		reply.Status = 409
		reply.Message = fmt.Sprintf("%s (%s)", jobs.ErrOneInstance, reason)
		return nil
	}

	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowBackup,
		args.Job.ID,
		"manual",
		"",
		jobs.BackupInput{SkipCheck: args.SkipCheck, Web: args.Web, ExtraExclusions: args.ExtraExclusions},
		[]string{"backup:" + args.Job.ID},
		args.Job.Retry+1,
		time.Duration(max(args.Job.RetryInterval, 1))*time.Minute,
	)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	execution, created, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	if created {
		if err := backup.PrepareQueue(s.Store, args.Job, args.Web); err != nil {
			log.Error(err, "jobrpc: failed to mint backup queued task", "backupID", args.Job.ID)
		}
	}
	reply.Status = 200
	reply.ExecutionID = execution.ID
	return nil
}

func (s *Service) RestoreQueue(args *RestoreQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if err := restore.CancelQueued(s.Store, args.Job); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
		return nil
	}

	if reason := s.blockedReason(jobs.WorkflowRestore, args.Job.ID, "target:"+args.Job.DestTarget.Name); reason != "" {
		reply.Status = 409
		reply.Message = fmt.Sprintf("%s (%s)", jobs.ErrOneInstance, reason)
		return nil
	}

	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowRestore,
		args.Job.ID,
		"manual",
		"",
		jobs.RestoreInput{SkipCheck: args.SkipCheck, Web: args.Web},
		[]string{"restore:" + args.Job.ID, "target:" + args.Job.DestTarget.Name},
		args.Job.Retry+1,
		time.Duration(max(args.Job.RetryInterval, 1))*time.Minute,
	)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	execution, created, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	if created {
		if err := restore.PrepareQueue(s.Store, args.Job, args.Web); err != nil {
			log.Error(err, "jobrpc: failed to mint restore queued task", "restoreID", args.Job.ID)
		}
	}
	reply.Status = 200
	reply.ExecutionID = execution.ID
	return nil
}

func (s *Service) MtfQueue(args *MtfJobQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if err := mtf.CancelQueued(s.Store, args.JobID); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
		return nil
	}

	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowMtfMigration,
		args.JobID,
		"manual",
		"",
		struct{}{},
		[]string{"mtf:" + args.JobID, "mtf-tape"},
		1,
		time.Minute,
	)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	execution, created, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	if created {
		if err := mtf.PrepareQueue(s.Store, args.JobID, args.Web); err != nil {
			log.Error(err, "jobrpc: failed to mint mtf queued task", "mtfJobID", args.JobID)
		}
	}
	reply.Status = 200
	reply.ExecutionID = execution.ID
	return nil
}

func RunServer(ctx context.Context, socketPath string, engine *jobs.Engine, app *application.Runtime) error {
	return rpcserver.Run(ctx, socketPath, ServiceName, &Service{
		ctx:    ctx,
		Store:  app,
		Engine: engine,
	})
}
