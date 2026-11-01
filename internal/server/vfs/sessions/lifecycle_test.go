//go:build linux

package sessions

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
)

func TestSupervisorClosesCanceledMounts(t *testing.T) {
	for _, kind := range []string{"agent", "s3"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				m, err := ReserveMount(ctx, t.Name(), "owner")
				if err != nil {
					t.Fatal(err)
				}
				fsCtx, fsCancel := context.WithCancel(m.Context())
				base := vfs.InjectBase(vfs.VFSBase{Ctx: fsCtx, Cancel: fsCancel})
				if kind == "agent" {
					fs := &arpcfs.ARPCFS{VFSBase: base}
					err = m.AttachARPCFS(fs)
					if GetSessionARPCFS(m.id) != fs {
						t.Fatal("agent filesystem not registered")
					}
				} else {
					fs := &s3fs.S3FS{VFSBase: base}
					err = m.AttachS3FS(fs)
					if GetSessionS3FS(m.id) != fs {
						t.Fatal("S3 filesystem not registered")
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				m.FinishSetup()
				cancel()
				synctest.Wait()
				if OwnsMount(m.id, "owner") || fsCtx.Err() == nil {
					t.Fatal("canceled mount was not reclaimed")
				}
			})
		})
	}
}

func TestSupervisorRetriesCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m, err := ReserveMount(ctx, t.Name(), "owner")
		if err != nil {
			t.Fatal(err)
		}
		attempts := 0
		m.SetCleanup(func(context.Context) error {
			attempts++
			if attempts == 1 {
				return errors.New("transient teardown failure")
			}
			return nil
		})
		m.FinishSetup()
		cancel()
		synctest.Wait()
		if attempts != 1 || !OwnsMount(m.id, "owner") {
			t.Fatal("failed cleanup did not retain its registration")
		}
		time.Sleep(sessionProbeInterval)
		synctest.Wait()
		if attempts != 2 || OwnsMount(m.id, "owner") {
			t.Fatal("supervisor did not retry and reclaim the mount")
		}
	})
}

func TestCannotAttachAfterSetupFinishes(t *testing.T) {
	m := reserveTestMount(t, "owner")
	m.FinishSetup()
	if err := m.AttachARPCFS(new(arpcfs.ARPCFS)); !errors.Is(err, ErrMountStale) {
		t.Fatalf("late agent attachment: %v", err)
	}
	if err := m.AttachS3FS(new(s3fs.S3FS)); !errors.Is(err, ErrMountStale) {
		t.Fatalf("late S3 attachment: %v", err)
	}
}
