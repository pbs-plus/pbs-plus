//go:build linux

package rpc

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/database"
	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

type BackupQueueArgs struct {
	Job             database.Backup
	SkipCheck       bool
	Web             bool
	Stop            bool
	ExtraExclusions []string
}

type RestoreQueueArgs struct {
	Job       database.Restore
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
	Status  int
	Message string
}

var BackupJobFactory func(database.Backup, *store.Store, bool, bool, []string) *jobs.Job

var RestoreJobFactory func(database.Restore, *store.Store, bool, bool) (*jobs.Job, error)

// MtfJobFactory creates an MTF tape → pxar migration job operation. Set
var MtfJobFactory func(string, *store.Store) (*jobs.Job, error)

type JobRPCService struct {
	ctx     context.Context
	Store   *store.Store
	Manager *jobs.Manager
}

func (s *JobRPCService) BackupQueue(args *BackupQueueArgs, reply *QueueReply) error {
	if args.Stop {
		err := s.Manager.StopJob(args.Job.ID)
		if err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}

		reply.Status = 200
		return nil
	}

	jobOp := BackupJobFactory(args.Job, s.Store, args.SkipCheck, args.Web, args.ExtraExclusions)
	if err := s.Manager.Enqueue(jobOp); err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	reply.Status = 200

	return nil
}

func (s *JobRPCService) RestoreQueue(args *RestoreQueueArgs, reply *QueueReply) error {
	if args.Stop {
		err := s.Manager.StopJob(args.Job.ID)
		if err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}

		reply.Status = 200
		return nil
	}

	jobOp, err := RestoreJobFactory(args.Job, s.Store, args.SkipCheck, args.Web)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	if err := s.Manager.Enqueue(jobOp); err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	reply.Status = 200

	return nil
}

func (s *JobRPCService) MtfQueue(args *MtfJobQueueArgs, reply *QueueReply) error {
	if args.Stop {
		if err := s.Manager.StopJob(args.JobID); err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}
		reply.Status = 200
		return nil
	}

	jobOp, err := MtfJobFactory(args.JobID, s.Store)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	if err := s.Manager.Enqueue(jobOp); err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}
	reply.Status = 200
	return nil
}

func StartJobRPCServer(watcher chan<- struct{}, ctx context.Context, socketPath string, manager *jobs.Manager, storeInstance *store.Store) error {
	if err := os.RemoveAll(socketPath); err != nil && !os.IsNotExist(err) {
		log.Error(err, "")
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", socketPath, err)
	}

	service := &JobRPCService{
		ctx:     ctx,
		Store:   storeInstance,
		Manager: manager,
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

func RunJobRPCServer(ctx context.Context, socketPath string, manager *jobs.Manager, storeInstance *store.Store) error {
	watcher := make(chan struct{}, 1)
	err := StartJobRPCServer(watcher, ctx, socketPath, manager, storeInstance)
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
