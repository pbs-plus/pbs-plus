//go:build linux
// +build linux

package rpclocker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/puzpuzpuz/xsync/v3"
)

type Args struct {
	Key string
}

type Reply struct {
	Success bool
}

type LockerRPC struct {
	locks *xsync.MapOf[string, *sync.Mutex]
}

func (s *LockerRPC) getOrCreateMutex(key string) *sync.Mutex {
	mu, _ := s.locks.LoadOrStore(key, new(sync.Mutex))
	return mu
}

func (s *LockerRPC) Lock(args *Args, reply *Reply) error {
	if args.Key == "" {
		reply.Success = false
		return errors.New("lock key cannot be empty")
	}

	keyMutex := s.getOrCreateMutex(args.Key)

	keyMutex.Lock()

	reply.Success = true
	return nil
}

func (s *LockerRPC) TryLock(args *Args, reply *Reply) error {
	if args.Key == "" {
		reply.Success = false
		return errors.New("lock key cannot be empty")
	}

	keyMutex := s.getOrCreateMutex(args.Key)

	acquired := keyMutex.TryLock()

	reply.Success = acquired
	return nil
}

func (s *LockerRPC) Unlock(args *Args, reply *Reply) error {
	if args.Key == "" {
		reply.Success = false
		return errors.New("lock key cannot be empty")
	}

	keyMutex, ok := s.locks.Load(args.Key)

	if !ok {
		reply.Success = false
		return nil // Or return specific error
	}

	keyMutex.Unlock()

	reply.Success = true
	return nil
}

func StartLockerServer(watcher chan struct{}, socketPath string) error {
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", socketPath, err)
	}

	service := &LockerRPC{
		locks: xsync.NewMapOf[string, *sync.Mutex](),
	}

	if err := rpc.Register(service); err != nil {
		_ = listener.Close()
		return fmt.Errorf("failed to register rpc service: %v", err)
	}

	syslog.L.Info().
		WithMessage("lock server starting").
		WithField("socket", socketPath).
		Write()

	ready := make(chan struct{})
	go func() {
		if watcher != nil {
			defer close(watcher)
		}
		close(ready)
		rpc.Accept(listener)
		syslog.L.Info().
			WithMessage("lock server stopped").
			WithField("socket", socketPath).
			Write()
		_ = listener.Close()
	}()

	<-ready

	return nil
}

func RunLockerServer(ctx context.Context, socketPath string) error {
	watcher := make(chan struct{}, 1)
	err := StartLockerServer(watcher, socketPath)
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		syslog.L.Info().
			WithMessage("lock server shutting down due to context cancellation").
			WithField("socket", socketPath).
			Write()
		_ = os.Remove(socketPath)
	case <-watcher:
		syslog.L.Info().
			WithMessage("lock server shut down unexpectedly").
			WithField("socket", socketPath).
			Write()
	}

	return nil
}
