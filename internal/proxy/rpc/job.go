//go:build linux
// +build linux

package rpcmount

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type UpdateArgs struct {
	Job types.Job
}

type UpdateReply struct {
	Status  int
	Message string
}

type JobRPCService struct {
	ctx   context.Context
	Store *store.Store
}

func (s *JobRPCService) Update(args *UpdateArgs, reply *UpdateReply) error {
	if err := s.Store.Database.UpdateJob(nil, args.Job); err != nil {
		reply.Status = 500
		reply.Message = err.Error()
	}

	return nil
}

func StartJobRPCServer(watcher chan struct{}, ctx context.Context, socketPath string, storeInstance *store.Store) error {
	// Remove any stale socket file.
	_ = os.RemoveAll(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", socketPath, err)
	}

	service := &JobRPCService{
		ctx:   ctx,
		Store: storeInstance,
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

func RunJobRPCServer(ctx context.Context, socketPath string, storeInstance *store.Store) error {
	watcher := make(chan struct{}, 1)
	err := StartJobRPCServer(watcher, ctx, socketPath, storeInstance)
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
