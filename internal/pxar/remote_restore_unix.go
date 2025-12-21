//go:build unix

package pxar

import (
	"context"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	_ = os.Chmod(path, os.FileMode(e.Mode&0777))
	_ = os.Chown(path, int(e.UID), int(e.GID))

	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		for name, value := range xattrs {
			_ = unix.Setxattr(path, name, value, 0)
		}
	}
	mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	_ = os.Chtimes(path, mt, mt)
	return nil
}

func remoteApplyMetaSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	_ = os.Lchown(path, int(e.UID), int(e.GID))

	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		for name, value := range xattrs {
			_ = unix.Lsetxattr(path, name, value, 0)
		}
	}
	return nil
}
