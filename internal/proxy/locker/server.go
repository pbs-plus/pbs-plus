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

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
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
// NOTE: This function assumes mapMutex is already held.
func (s *LockerRPC) getOrCreateMutex(key string) *sync.Mutex {
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

	s.mapMutex.Lock()
	keyMutex := s.getOrCreateMutex(args.Key)
	s.mapMutex.Unlock()

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

	s.mapMutex.Lock()
	keyMutex := s.getOrCreateMutex(args.Key)
	s.mapMutex.Unlock()

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

	reply.Success = true
	syslog.L.Info().WithMessage("lock released").WithField("key", args.Key).Write()
	return nil
}

func StartLockerServer(ctx context.Context) error {
	_ = os.RemoveAll(constants.LockSocketPath)
	listener, err := net.Listen("unix", constants.LockSocketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", constants.LockSocketPath, err)
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
		WithField("socket", constants.LockSocketPath).
		Write()

	go func() {
		rpc.Accept(listener)
	}()

	<-ctx.Done()

	return nil
}
