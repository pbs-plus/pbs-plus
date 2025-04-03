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
)

// Args holds the key for the lock operation.
type Args struct {
	Key string
}

// Reply indicates the success of the lock operation.
type Reply struct {
	Success bool
}

// LockerRPC implements the RPC service for locking.
type LockerRPC struct {
	// mapMutex protects access to the locks map itself.
	mapMutex sync.Mutex
	// locks stores the actual mutex for each key.
	locks map[string]*sync.Mutex
}

// getOrCreateMutex retrieves the mutex for a key, creating it if necessary.
func (s *LockerRPC) getOrCreateMutex(key string) *sync.Mutex {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	mu, ok := s.locks[key]
	if !ok {
		mu = &sync.Mutex{}
		s.locks[key] = mu
	}
	return mu
}

// Lock acquires the lock for the given key, blocking until available.
func (s *LockerRPC) Lock(args *Args, reply *Reply) error {
	if args.Key == "" {
		reply.Success = false
		return errors.New("lock key cannot be empty")
	}

	keyMutex := s.getOrCreateMutex(args.Key)

	keyMutex.Lock()

	reply.Success = true
	syslog.L.Info().WithMessage("lock acquired").WithField("key", args.Key).Write()
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
	if acquired {
		syslog.L.Info().WithMessage("trylock succeeded").WithField("key", args.Key).Write()
	} else {
		syslog.L.Info().WithMessage("trylock failed").WithField("key", args.Key).Write()
	}
	return nil
}

// Unlock releases the lock for the given key.
func (s *LockerRPC) Unlock(args *Args, reply *Reply) error {
	if args.Key == "" {
		reply.Success = false
		return errors.New("lock key cannot be empty")
	}

	s.mapMutex.Lock()
	keyMutex, ok := s.locks[args.Key]
	s.mapMutex.Unlock()

	if !ok {
		reply.Success = false
		syslog.L.Warn().
			WithMessage("unlock attempted on non-existent or never-locked key").
			WithField("key", args.Key).
			Write()
		return fmt.Errorf("key '%s' not found or never locked", args.Key)
	}

	keyMutex.Unlock()

	s.mapMutex.Lock()
	delete(s.locks, args.Key)
	s.mapMutex.Unlock()

	reply.Success = true
	syslog.L.Info().WithMessage("lock released").WithField("key", args.Key).Write()
	return nil
}

func StartLockerServer(watcher chan struct{}, socketPath string) error {
	_ = os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", socketPath, err)
	}

	service := &LockerRPC{
		locks: make(map[string]*sync.Mutex),
	}

	if err := rpc.Register(service); err != nil {
		_ = listener.Close() // Clean up listener on registration error
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
	case <-watcher:
	}

	return nil
}
