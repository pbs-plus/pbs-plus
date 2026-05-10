//go:build linux

package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_ClosedByDefault(t *testing.T) {
	cb := NewCircuitBreaker("test", 3, time.Second)
	if cb.IsOpen() {
		t.Error("expected circuit to be closed initially")
	}
}

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker("test", 3, 100*time.Millisecond)
	testErr := errors.New("test error")

	for i := range 3 {
		err := cb.Call(func() error { return testErr })
		if !errors.Is(err, testErr) {
			t.Fatalf("attempt %d: expected testErr, got %v", i, err)
		}
	}

	if !cb.IsOpen() {
		t.Error("expected circuit to be open after threshold failures")
	}

	err := cb.Call(func() error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestCircuitBreaker_ResetsOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker("test", 3, 100*time.Millisecond)
	testErr := errors.New("test error")

	// Two failures shouldn't open the circuit
	_ = cb.Call(func() error { return testErr })
	_ = cb.Call(func() error { return testErr })

	if cb.IsOpen() {
		t.Error("circuit should not be open after 2 failures")
	}

	// Success resets
	err := cb.Call(func() error { return nil })
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if cb.IsOpen() {
		t.Error("circuit should be closed after success")
	}
}

func TestCircuitBreaker_HalfOpensAfterDuration(t *testing.T) {
	cb := NewCircuitBreaker("test", 2, 50*time.Millisecond)
	testErr := errors.New("test error")

	_ = cb.Call(func() error { return testErr })
	_ = cb.Call(func() error { return testErr })

	if !cb.IsOpen() {
		t.Fatal("expected circuit to be open")
	}

	time.Sleep(60 * time.Millisecond)

	if cb.IsOpen() {
		t.Error("expected circuit to be half-open after duration")
	}

	// Should accept calls again
	err := cb.Call(func() error { return nil })
	if err != nil {
		t.Errorf("expected nil after circuit half-open, got %v", err)
	}
}

func TestWithRetry_SucceedsFirstAttempt(t *testing.T) {
	ctx := context.Background()
	calls := 0
	err := WithRetry(ctx, 3, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

func TestWithRetry_NonRetryableError(t *testing.T) {
	ctx := context.Background()
	nonRetryable := errors.New("validation failed")
	calls := 0
	err := WithRetry(ctx, 3, func() error {
		calls++
		return nonRetryable
	})
	if !errors.Is(err, nonRetryable) {
		t.Errorf("expected nonRetryable, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (no retry), got %d", calls)
	}
}

func TestWithRetry_RespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := WithRetry(ctx, 3, func() error {
		return context.Canceled
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestIsRetryable(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"context canceled", context.Canceled, true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"random error", errors.New("something broke"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRetryable(tt.err)
			if got != tt.expected {
				t.Errorf("IsRetryable(%v) = %v, want %v", tt.err, got, tt.expected)
			}
		})
	}
}
