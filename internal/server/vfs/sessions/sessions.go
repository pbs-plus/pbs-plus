//go:build linux

package sessions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/pxar"
	"github.com/pbs-plus/pbs-plus/internal/safemap"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
)

type FSMount struct {
	id        string
	owner     string
	arpcfs    *arpcfs.ARPCFS
	s3fs      *s3fs.S3FS
	pxar      *pxar.PxarReader
	cleanup   func(context.Context) error
	ctx       context.Context
	cancel    context.CancelFunc
	ready     chan struct{}
	closed    chan struct{}
	teardown  chan struct{}
	setupOnce sync.Once
}

var (
	activeMounts  = safemap.New[string, *FSMount]()
	registerMu    sync.Mutex
	ErrMountInUse = errors.New("a mount already owns this backup")
	ErrMountStale = errors.New("mount reservation no longer owns this backup")
)

const sessionProbeInterval = 5 * time.Second

// ReserveMount retains exclusive ownership through setup, remote cleanup and detach, including failed teardown attempts.
func ReserveMount(ctx context.Context, connID, owner string) (*FSMount, error) {
	registerMu.Lock()
	defer registerMu.Unlock()
	if _, ok := activeMounts.Get(connID); ok {
		return nil, fmt.Errorf("%w: %s", ErrMountInUse, connID)
	}
	ctx, cancel := context.WithCancel(ctx)
	m := &FSMount{
		id: connID, owner: owner, ctx: ctx, cancel: cancel,
		ready: make(chan struct{}), closed: make(chan struct{}), teardown: make(chan struct{}, 1),
	}
	activeMounts.Set(connID, m)
	go m.supervise()
	return m, nil
}

// FinishSetup must run even when construction fails so teardown can safely take ownership of partially initialized resources.
func (m *FSMount) FinishSetup() { m.setupOnce.Do(func() { close(m.ready) }) }

func (m *FSMount) SetCleanup(cleanup func(context.Context) error) { m.cleanup = cleanup }

func (m *FSMount) Context() context.Context { return m.ctx }

func (m *FSMount) AttachARPCFS(fs *arpcfs.ARPCFS) error {
	registerMu.Lock()
	defer registerMu.Unlock()
	if cur, ok := activeMounts.Get(m.id); !ok || cur != m || !m.empty() {
		return ErrMountStale
	}
	m.arpcfs = fs
	return nil
}

func (m *FSMount) AttachS3FS(fs *s3fs.S3FS) error {
	registerMu.Lock()
	defer registerMu.Unlock()
	if cur, ok := activeMounts.Get(m.id); !ok || cur != m || !m.empty() {
		return ErrMountStale
	}
	m.s3fs = fs
	return nil
}

func (m *FSMount) empty() bool { return m.arpcfs == nil && m.s3fs == nil && m.pxar == nil }

func (m *FSMount) Disconnect(ctx context.Context) error {
	m.cancel()
	select {
	case <-m.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case m.teardown <- struct{}{}:
		defer func() { <-m.teardown }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registerMu.Lock()
	cur, ok := activeMounts.Get(m.id)
	registerMu.Unlock()
	if !ok || cur != m {
		return nil
	}
	if m.arpcfs != nil {
		if err := m.arpcfs.Unmount(ctx); err != nil {
			return err
		}
	}
	if m.s3fs != nil {
		if err := m.s3fs.Unmount(ctx); err != nil {
			return err
		}
	}
	if m.pxar != nil {
		if err := m.pxar.Close(); err != nil {
			return err
		}
	}
	if m.cleanup != nil {
		if err := m.cleanup(ctx); err != nil {
			return err
		}
	}
	registerMu.Lock()
	defer registerMu.Unlock()
	if cur, ok := activeMounts.Get(m.id); ok && cur == m {
		activeMounts.Del(m.id)
		close(m.closed)
	}
	return nil
}

func DisconnectSession(ctx context.Context, connID, owner string) error {
	registerMu.Lock()
	m, ok := activeMounts.Get(connID)
	registerMu.Unlock()
	if !ok || m.owner != owner {
		return nil
	}
	return m.Disconnect(ctx)
}

func OwnsMount(connID, owner string) bool {
	registerMu.Lock()
	defer registerMu.Unlock()
	m, ok := activeMounts.Get(connID)
	return ok && m.owner == owner
}

type sessionWatch struct{ downSince time.Time }

func (w *sessionWatch) observe(connected bool, now time.Time, grace time.Duration) bool {
	if connected {
		w.downSince = time.Time{}
		return false
	}
	if w.downSince.IsZero() {
		w.downSince = now
		return false
	}
	return now.Sub(w.downSince) >= grace
}

func (m *FSMount) supervise() {
	<-m.ready
	ticker := time.NewTicker(sessionProbeInterval)
	defer ticker.Stop()
	done := m.ctx.Done()
	stopping := m.empty()
	var watch sessionWatch
	for {
		select {
		case <-m.closed:
			return
		case <-done:
			stopping = true
			done = nil
		case <-ticker.C:
		}
		if !stopping && m.arpcfs != nil {
			stopping = watch.observe(m.arpcfs.SessionConnected(), time.Now(), arpcfs.SessionGrace)
		}
		if !stopping {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := m.Disconnect(ctx)
		cancel()
		if err == nil {
			return
		}
		log.Error(err, "mount teardown failed, retaining ownership for retry", "connId", m.id)
	}
}

func NewPxarReader(ctx context.Context, connID string, r *pxar.PxarReader) (*FSMount, error) {
	m, err := ReserveMount(ctx, connID, "")
	if err != nil {
		return nil, err
	}
	registerMu.Lock()
	m.pxar = r
	registerMu.Unlock()
	m.FinishSetup()
	return m, nil
}

func GetSessionARPCFS(connID string) *arpcfs.ARPCFS {
	registerMu.Lock()
	defer registerMu.Unlock()
	if m, ok := activeMounts.Get(connID); ok {
		return m.arpcfs
	}
	return nil
}

func GetSessionS3FS(connID string) *s3fs.S3FS {
	registerMu.Lock()
	defer registerMu.Unlock()
	if m, ok := activeMounts.Get(connID); ok {
		return m.s3fs
	}
	return nil
}

func GetSessionPxarReader(connID string) *pxar.PxarReader {
	registerMu.Lock()
	defer registerMu.Unlock()
	if m, ok := activeMounts.Get(connID); ok {
		return m.pxar
	}
	return nil
}
