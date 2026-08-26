//go:build linux

package jobrpc

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/application"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
)

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
		[]string{"mtf:" + args.JobID},
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

func StartServer(watcher chan<- struct{}, ctx context.Context, socketPath string, engine *jobs.Engine, app *application.Runtime) error {
	if err := os.RemoveAll(socketPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", socketPath, err)
	}

	service := &Service{
		ctx:    ctx,
		Store:  app,
		Engine: engine,
	}

	if err := rpc.Register(service); err != nil {
		return fmt.Errorf("failed to register rpc service: %w", err)
	}

	ready := make(chan struct{})

	go func() {
		if watcher != nil {
			defer close(watcher)
		}
		close(ready)
		rpc.Accept(listener)
	}()
	log.Info("rPC server listening",
		"socket", socketPath)

	<-ready

	return nil
}

func RunServer(ctx context.Context, socketPath string, engine *jobs.Engine, app *application.Runtime) error {
	watcher := make(chan struct{}, 1)
	err := StartServer(watcher, ctx, socketPath, engine, app)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		log.Info("rpc mount server shutting down due to context cancellation",
			"socket", socketPath)

		if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	case <-watcher:
		log.Info("rpc mount server shut down unexpectedly",
			"socket", socketPath)

	}

	return nil
}
