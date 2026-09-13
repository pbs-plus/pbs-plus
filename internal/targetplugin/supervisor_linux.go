package targetplugin

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Supervisor bounds and reaps one-process-per-operation plugin execution.
type Supervisor struct {
	globalSlots    chan struct{}
	perPluginLimit int

	mu      sync.Mutex
	plugins map[string]*pluginLimiter
}

type pluginLimiter struct {
	slots chan struct{}
	users int
}

func NewSupervisor(globalLimit, perPluginLimit int) (*Supervisor, error) {
	if globalLimit < 1 || perPluginLimit < 1 {
		return nil, errors.New("plugin process limits must be positive")
	}
	if perPluginLimit > globalLimit {
		return nil, errors.New("per-plugin process limit cannot exceed global limit")
	}
	return &Supervisor{
		globalSlots:    make(chan struct{}, globalLimit),
		perPluginLimit: perPluginLimit,
		plugins:        make(map[string]*pluginLimiter),
	}, nil
}

// Run waits for capacity, starts one plugin process, invokes operation, and reaps the process.
func (s *Supervisor) Run(ctx context.Context, pluginID, executable string, operation func(context.Context, *Process) error, args ...string) error {
	if s == nil {
		return errors.New("plugin supervisor is nil")
	}
	if operation == nil {
		return errors.New("plugin operation is required")
	}
	if err := validateIdentifier("plugin ID", pluginID, maxPluginIDLength); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	releasePlugin, err := s.acquirePlugin(ctx, pluginID)
	if err != nil {
		return err
	}
	defer releasePlugin()

	select {
	case s.globalSlots <- struct{}{}:
		defer func() { <-s.globalSlots }()
	case <-ctx.Done():
		return ctx.Err()
	}

	process, err := Start(ctx, executable, args...)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = process.Close()
		}
	}()

	operationErr := operation(ctx, process)
	closeErr := process.Close()
	closed = true
	if err := errors.Join(operationErr, closeErr); err != nil {
		return fmt.Errorf("run plugin %q: %w", pluginID, err)
	}
	return nil
}

func (s *Supervisor) acquirePlugin(ctx context.Context, pluginID string) (func(), error) {
	s.mu.Lock()
	limiter := s.plugins[pluginID]
	if limiter == nil {
		limiter = &pluginLimiter{slots: make(chan struct{}, s.perPluginLimit)}
		s.plugins[pluginID] = limiter
	}
	limiter.users++
	s.mu.Unlock()

	select {
	case limiter.slots <- struct{}{}:
		return func() {
			<-limiter.slots
			s.releasePlugin(pluginID, limiter)
		}, nil
	case <-ctx.Done():
		s.releasePlugin(pluginID, limiter)
		return nil, ctx.Err()
	}
}

func (s *Supervisor) releasePlugin(pluginID string, limiter *pluginLimiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	limiter.users--
	if limiter.users == 0 {
		delete(s.plugins, pluginID)
	}
}
