package arpcfs

import (
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const sourceCheckTimeout = 30 * time.Second

func (fs *ARPCFS) sourceRoot() string {
	sub := strings.Trim(strings.ReplaceAll(fs.Backup.Subpath, "\\", "/"), "/")
	return path.Clean("/" + sub)
}

// statSourceRoot asks the agent directly, bypassing the attribute cache that would still remember the root.
func (fs *ARPCFS) statSourceRoot(ctx context.Context, root string) error {
	if fs.sourceStat != nil {
		return fs.sourceStat(ctx, root)
	}
	pipe, err := fs.getPipe(ctx)
	if err != nil {
		return fs.sessionLost(err)
	}
	ctxN, cancel := context.WithTimeout(ctx, sourceCheckTimeout)
	defer cancel()
	_, err = pipe.CallData(ctxN, "Attr", &fswire.StatReq{Path: root})
	return err
}

// noteMissing flags a vanished source (drive unplugged or hung) as a task warning, leaving the errno unchanged
// so the snapshot still keeps everything already read instead of failing.
func (fs *ARPCFS) noteMissing(ctx context.Context, p string, err error) {
	if err == nil || fs.sourceGone.Load() || isIgnoredPath(p) || errors.Is(err, ErrSessionLost) || ctx.Err() != nil {
		return
	}
	root := fs.sourceRoot()
	statErr := fs.statSourceRoot(ctx, root)
	if statErr == nil {
		return
	}
	if _, ok := errors.AsType[*os.PathError](statErr); !ok {
		log.Debug("source root check inconclusive", "backupID", fs.Backup.ID, "root", root, "error", statErr.Error())
		return
	}
	if !fs.sourceGone.CompareAndSwap(false, true) {
		return
	}
	l := fs.jobLog
	if l == nil {
		l = log.L
	}
	l.Warn("backup source disappeared during the backup; this snapshot is missing everything not read before then",
		"source", root, "first_missing", p, "error", statErr.Error())
}
