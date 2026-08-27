//go:build linux

package jobrpc

import (
	"context"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
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

func (s *Service) BackupQueue(args *BackupQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if _, err := s.Engine.CancelDefinition(s.ctx, jobs.WorkflowBackup, args.Job.ID); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
		return nil
	}

	request, err := jobs.NewWorkflowSubmit(
		jobs.WorkflowBackup,
		args.Job.ID,
		"manual",
		"",
		jobs.BackupInput{SkipCheck: args.SkipCheck, Web: args.Web, ExtraExclusions: args.ExtraExclusions},
		[]string{"backup:" + args.Job.ID, "target:" + args.Job.Target.Name},
		args.Job.Retry+1,
		time.Duration(max(args.Job.RetryInterval, 1))*time.Minute,
	)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	execution, _, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	reply.Status = 200
	reply.ExecutionID = execution.ID
	return nil
}

func (s *Service) RestoreQueue(args *RestoreQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if _, err := s.Engine.CancelDefinition(s.ctx, jobs.WorkflowRestore, args.Job.ID); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
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
	execution, _, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	reply.Status = 200
	reply.ExecutionID = execution.ID
	return nil
}

func (s *Service) MtfQueue(args *MtfJobQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if _, err := s.Engine.CancelDefinition(s.ctx, jobs.WorkflowMtfMigration, args.JobID); err != nil {
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
	execution, _, err := s.Engine.Submit(s.ctx, request)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
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
