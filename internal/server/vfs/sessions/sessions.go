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
	arpcfs "github.com/pbs-plus/pbs-plus/internal/server/vfs/arpcfs"
	s3fs "github.com/pbs-plus/pbs-plus/internal/server/vfs/s3fs"
)

type FSMount struct {
	sync.Mutex
	arpcfs *arpcfs.ARPCFS
	s3fs   *s3fs.S3FS
	pxar   *pxar.PxarReader
}

var activeMounts = safemap.New[string, *FSMount]()

const sessionProbeInterval = 5 * time.Second

// ErrMountInUse reports that a live mount already owns this connection ID.
var ErrMountInUse = errors.New("a live mount already exists for this backup")

var registerMu sync.Mutex

// ReserveMount claims connId before the caller builds and mounts its
// filesystem. Two runs of one backup share a connection ID, mount path and
// agent session, so the second must be refused before it force-detaches the
// first run's mount. The returned release undoes an unfilled reservation.
func ReserveMount(connId string) (release func(), err error) {
	registerMu.Lock()
	defer registerMu.Unlock()

	if existing, ok := activeMounts.Get(connId); ok {
		if !existing.shutDown() {
			return nil, fmt.Errorf("%w: %s", ErrMountInUse, connId)
		}
		log.Warn("replacing a shut-down mount registration", "connId", connId)
	}

	activeMounts.Set(connId, &FSMount{})

	var once sync.Once
	return func() {
		once.Do(func() {
			registerMu.Lock()
			defer registerMu.Unlock()
			if cur, ok := activeMounts.Get(connId); ok && cur.empty() {
				activeMounts.Del(connId)
			}
		})
	}, nil
}

func (m *FSMount) empty() bool {
	return m.arpcfs == nil && m.s3fs == nil && m.pxar == nil
}

// shutDown reports whether this registration may be replaced. An in-flight
// reservation holds no filesystem yet and must still block a second claim.
func (m *FSMount) shutDown() bool {
	switch {
	case m.arpcfs != nil:
		return m.arpcfs.ShutDown()
	case m.s3fs != nil:
		return m.s3fs.ShutDown()
	default:
		return false
	}
}

// AttachARPCFS fills a reservation made by ReserveMount.
func AttachARPCFS(connId string, fs *arpcfs.ARPCFS) {
	registerMu.Lock()
	activeMounts.Set(connId, &FSMount{arpcfs: fs})
	registerMu.Unlock()

	go superviseARPCFS(connId, fs)
}

// AttachS3FS fills a reservation made by ReserveMount. S3 mounts have no agent
// transport, so there is no session to supervise.
func AttachS3FS(connId string, fs *s3fs.S3FS) {
	registerMu.Lock()
	activeMounts.Set(connId, &FSMount{s3fs: fs})
	registerMu.Unlock()
}

type sessionWatch struct {
	downSince time.Time
}

// observe records one liveness sample and reports whether the transport has
// been missing for longer than grace. A reconnect at any point clears the
// timer, so only an uninterrupted outage expires the mount.
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

func superviseARPCFS(connId string, fs *arpcfs.ARPCFS) {
	ticker := time.NewTicker(sessionProbeInterval)
	defer ticker.Stop()

	var watch sessionWatch

	for {
		select {
		case <-fs.Done():
			return
		case <-ticker.C:
		}

		if !watch.observe(fs.SessionConnected(), time.Now(), arpcfs.SessionGrace) {
			continue
		}

		log.Error(arpcfs.ErrSessionLost,
			"agent gone past grace period, tearing down mount",
			"connId", connId, "grace", arpcfs.SessionGrace.String())
		DisconnectSession(connId)
		return
	}
}

func NewPxarReader(connId string, r *pxar.PxarReader) {
	conn := &FSMount{
		pxar: r,
	}

	activeMounts.Set(connId, conn)
}

func DisconnectSession(connId string) {
	registerMu.Lock()
	fs, ok := activeMounts.GetAndDel(connId)
	registerMu.Unlock()

	if ok {
		if fs.arpcfs != nil {
			fs.arpcfs.Unmount(context.Background())
		}
		if fs.s3fs != nil {
			fs.s3fs.Unmount(context.Background())
		}
		if fs.pxar != nil {
			if err := fs.pxar.Close(); err != nil {
				log.Error(err, "")
			}
		}
	}
}

func GetSessionARPCFS(connId string) *arpcfs.ARPCFS {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.arpcfs
	} else {
		return nil
	}
}

func GetSessionS3FS(connId string) *s3fs.S3FS {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.s3fs
	} else {
		return nil
	}
}

func GetSessionPxarReader(connId string) *pxar.PxarReader {
	if conn, ok := activeMounts.Get(connId); ok {
		return conn.pxar
	} else {
		return nil
	}
}
