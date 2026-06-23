//go:build linux

package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

// CircuitBreaker prevents cascading failures when a downstream dependency
type CircuitBreaker struct {
	mu            sync.Mutex
	failures      int
	threshold     int
	halfOpenAfter time.Duration
	openUntil     time.Time
	name          string
}

func NewCircuitBreaker(name string, threshold int, halfOpenAfter time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		name:          name,
		threshold:     threshold,
		halfOpenAfter: halfOpenAfter,
	}
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()
	if cb.failures >= cb.threshold && time.Now().Before(cb.openUntil) {
		cb.mu.Unlock()
		return ErrCircuitOpen
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		if cb.failures >= cb.threshold {
			cb.openUntil = time.Now().Add(cb.halfOpenAfter)
			syslog.L.Warn().
				WithField("circuit", cb.name).
				WithField("failures", cb.failures).
				WithMessage(fmt.Sprintf("circuit opened until %s", cb.openUntil.Format(time.RFC3339))).
				Write()
		}
		return err
	}

	if cb.failures > 0 {
		syslog.L.Info().
			WithField("circuit", cb.name).
			WithMessage("circuit closed, failures reset").
			Write()
	}
	cb.failures = 0
	return nil
}

func (cb *CircuitBreaker) IsOpen() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures >= cb.threshold && time.Now().Before(cb.openUntil)
}

// IsRetryable returns true for errors that are safe to retry.
// Network errors, timeouts, and deadlocks are retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}

// WithRetry executes fn up to maxAttempts times with exponential backoff.
func WithRetry(ctx context.Context, maxAttempts int, fn func() error) error {
	var err error
	for attempt := range maxAttempts {
		if err = fn(); err == nil {
			return nil
		}

		if !IsRetryable(err) {
			return err
		}

		if attempt == maxAttempts-1 {
			break
		}

		wait := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, err)
}
