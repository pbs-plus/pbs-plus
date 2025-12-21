//go:build linux
// +build linux

package jobrpc

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/backend/backup"
	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type BackupQueueArgs struct {
	Job             types.Job
	SkipCheck       bool
	Web             bool
	Stop            bool
	ExtraExclusions []string
}

type RestoreQueueArgs struct {
	Job             types.Restore
	SkipCheck       bool
	Web             bool
	Stop            bool
	ExtraExclusions []string
}

type QueueReply struct {
	Status  int
	Message string
}

type JobRPCService struct {
	ctx           context.Context
	Store         *store.Store
	BackupManager *backup.Manager
}

func (s *JobRPCService) BackupQueue(args *BackupQueueArgs, reply *QueueReply) error {
	if args.Stop {
		err := s.BackupManager.StopJob(args.Job.ID)
		if err != nil {
			reply.Status = 500
			reply.Message = err.Error()
			return nil
		}

		reply.Status = 200
		return nil
	}

	job, err := backup.NewJob(args.Job, s.Store, args.SkipCheck, args.Web, args.ExtraExclusions)
	if err != nil {
		reply.Status = 500
		reply.Message = err.Error()
		return nil
	}

	s.BackupManager.Enqueue(job)
	reply.Status = 200

	return nil
}

func (s *JobRPCService) RestoreQueue(args *RestoreQueueArgs, reply *QueueReply) error {
	if args.Stop {
	}

	// TODO: restore queue operation

	reply.Status = 200

	return nil
}

func StartJobRPCServer(watcher chan struct{}, ctx context.Context, socketPath string, manager *backup.Manager, storeInstance *store.Store) error {
	// Remove any stale socket file.
	_ = os.RemoveAll(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", socketPath, err)
	}

	service := &JobRPCService{
		ctx:           ctx,
		Store:         storeInstance,
		BackupManager: manager,
	}

	// Register the RPC service.
	if err := rpc.Register(service); err != nil {
		return fmt.Errorf("failed to register rpc service: %v", err)
	}

	// Start accepting connections.
	ready := make(chan struct{})

	go func() {
		if watcher != nil {
			defer close(watcher)
		}
		close(ready)
		rpc.Accept(listener)
	}()

	syslog.L.Info().
		WithMessage("RPC server listening").
		WithField("socket", socketPath).
		Write()

	<-ready

	return nil
}

func RunJobRPCServer(ctx context.Context, socketPath string, manager *backup.Manager, storeInstance *store.Store) error {
	watcher := make(chan struct{}, 1)
	err := StartJobRPCServer(watcher, ctx, socketPath, manager, storeInstance)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		syslog.L.Info().
			WithMessage("rpc mount server shutting down due to context cancellation").
			WithField("socket", socketPath).
			Write()
		_ = os.Remove(socketPath)
	case <-watcher:
		syslog.L.Info().
			WithMessage("rpc mount server shut down unexpectedly").
			WithField("socket", socketPath).
			Write()
	}

	return nil
}
