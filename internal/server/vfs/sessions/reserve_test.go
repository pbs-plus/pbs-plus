//go:build linux

package sessions

import (
	"context"
	"errors"
	"testing"

	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
)

func liveMount() *FSMount {
	return &FSMount{
		arpcfs: &arpcfs.ARPCFS{
			VFSBase: vfs.InjectBase(vfs.VFSBase{Ctx: context.Background()}),
		},
	}
}

func shutDownMount(t *testing.T) *FSMount {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return &FSMount{
		arpcfs: &arpcfs.ARPCFS{VFSBase: vfs.InjectBase(vfs.VFSBase{Ctx: ctx})},
	}
}

func TestReserveMountRefusesSecondClaim(t *testing.T) {
	const id = "host|BACKUP-A"
	t.Cleanup(func() { activeMounts.Del(id) })

	release, err := ReserveMount(id)
	if err != nil {
		t.Fatalf("first reservation failed: %v", err)
	}

	if _, err := ReserveMount(id); !errors.Is(err, ErrMountInUse) {
		t.Fatalf("second claim against an in-flight reservation = %v, want ErrMountInUse", err)
	}

	release()

	if _, err := ReserveMount(id); err != nil {
		t.Fatalf("reservation after release failed: %v", err)
	}
}

func TestReserveMountBlocksWhileFilesystemLive(t *testing.T) {
	const id = "host|BACKUP-B"
	t.Cleanup(func() { activeMounts.Del(id) })

	release, err := ReserveMount(id)
	if err != nil {
		t.Fatalf("reservation failed: %v", err)
	}

	activeMounts.Set(id, liveMount())

	if _, err := ReserveMount(id); !errors.Is(err, ErrMountInUse) {
		t.Fatalf("claim against a live mount = %v, want ErrMountInUse", err)
	}

	release()
	if _, ok := activeMounts.Get(id); !ok {
		t.Fatal("release evicted a filled registration")
	}
}

func TestReleaseIsIdempotent(t *testing.T) {
	const id = "host|BACKUP-C"
	t.Cleanup(func() { activeMounts.Del(id) })

	release, err := ReserveMount(id)
	if err != nil {
		t.Fatalf("reservation failed: %v", err)
	}
	release()

	other, err := ReserveMount(id)
	if err != nil {
		t.Fatalf("re-reservation failed: %v", err)
	}

	release()
	if _, ok := activeMounts.Get(id); !ok {
		t.Fatal("stale release evicted a newer reservation")
	}
	other()
}

func TestEmptyAndShutDownClassification(t *testing.T) {
	if !(&FSMount{}).empty() {
		t.Fatal("a bare reservation should report empty")
	}
	if (&FSMount{}).shutDown() {
		t.Fatal("an in-flight reservation must not report shut down")
	}
	if (&FSMount{pxar: new(pxar.PxarReader)}).shutDown() {
		t.Fatal("a pxar reader registration must not be replaceable")
	}
	if !shutDownMount(t).shutDown() {
		t.Fatal("a torn-down filesystem should be replaceable")
	}
}

func TestReserveMountReplacesShutDownRegistration(t *testing.T) {
	const id = "host|BACKUP-D"
	t.Cleanup(func() { activeMounts.Del(id) })

	activeMounts.Set(id, shutDownMount(t))

	release, err := ReserveMount(id)
	if err != nil {
		t.Fatalf("reservation blocked by a shut-down mount: %v", err)
	}
	release()
}
