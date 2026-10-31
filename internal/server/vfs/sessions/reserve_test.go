//go:build linux

package sessions

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
)

func reserveTestMount(t *testing.T, owner string) *FSMount {
	t.Helper()
	m, err := ReserveMount(context.Background(), t.Name(), owner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		m.FinishSetup()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := m.Disconnect(ctx); err != nil {
			t.Error(err)
		}
	})
	return m
}

func TestReserveMountRefusesSecondClaim(t *testing.T) {
	m := reserveTestMount(t, "first")
	if _, err := ReserveMount(context.Background(), m.id, "second"); !errors.Is(err, ErrMountInUse) {
		t.Fatalf("second reservation: %v", err)
	}
}

func TestStaleCleanupCannotCancelCurrentOwner(t *testing.T) {
	m := reserveTestMount(t, "current")
	m.FinishSetup()
	if err := DisconnectSession(context.Background(), m.id, "old"); err != nil {
		t.Fatal(err)
	}
	if !OwnsMount(m.id, "current") || m.Context().Err() != nil {
		t.Fatal("stale cleanup changed the current reservation")
	}
}

func TestOldReservationCannotRemoveOrAttachToReplacement(t *testing.T) {
	old := reserveTestMount(t, "old")
	old.FinishSetup()
	if err := old.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	current := reserveTestMount(t, "current")
	if err := old.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := old.AttachARPCFS(new(arpcfs.ARPCFS)); !errors.Is(err, ErrMountStale) {
		t.Fatalf("stale attachment: %v", err)
	}
	if !OwnsMount(current.id, "current") {
		t.Fatal("old reservation evicted its replacement")
	}
}

func TestOwnershipHeldUntilCleanupCompletes(t *testing.T) {
	m := reserveTestMount(t, "first")
	entered, release := make(chan struct{}), make(chan struct{})
	m.SetCleanup(func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	m.FinishSetup()
	done := make(chan error, 1)
	go func() { done <- m.Disconnect(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("cleanup did not start")
	}
	_, err := ReserveMount(context.Background(), m.id, "second")
	close(release)
	if !errors.Is(err, ErrMountInUse) {
		t.Fatalf("reservation during teardown: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	reserveTestMount(t, "second")
}

func TestFailedCleanupRetainsOwnershipForRetry(t *testing.T) {
	m := reserveTestMount(t, "first")
	var allow atomic.Bool
	failure := errors.New("detach failed")
	m.SetCleanup(func(context.Context) error {
		if !allow.Load() {
			return failure
		}
		return nil
	})
	m.FinishSetup()
	if err := m.Disconnect(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first cleanup: %v", err)
	}
	_, err := ReserveMount(context.Background(), m.id, "second")
	allow.Store(true)
	if !errors.Is(err, ErrMountInUse) {
		t.Fatalf("failed teardown released its reservation: %v", err)
	}
	if err := m.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
	reserveTestMount(t, "second")
}

func TestCleanupCancelsSetupWithoutReleasingOwnership(t *testing.T) {
	m := reserveTestMount(t, "first")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := m.Disconnect(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup during pending setup: %v", err)
	}
	if !errors.Is(m.Context().Err(), context.Canceled) || !OwnsMount(m.id, "first") {
		t.Fatal("setup was not canceled or ownership was released early")
	}
	m.FinishSetup()
	if err := m.Disconnect(context.Background()); err != nil {
		t.Fatal(err)
	}
}
