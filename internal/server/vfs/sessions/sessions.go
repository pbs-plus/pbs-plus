//go:build linux

package sessions

import (
	"context"
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

func NewARPCFSMount(connId string, fs *arpcfs.ARPCFS) {
	conn := &FSMount{
		arpcfs: fs,
	}

	activeMounts.Set(connId, conn)
	go superviseARPCFS(connId, fs)
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

func NewS3FSMount(connId string, fs *s3fs.S3FS) {
	conn := &FSMount{
		s3fs: fs,
	}

	activeMounts.Set(connId, conn)
}

func DisconnectSession(connId string) {
	if fs, ok := activeMounts.GetAndDel(connId); ok {
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
