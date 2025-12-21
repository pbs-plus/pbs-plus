//go:build windows

package pxar

import (
	"context"
	"os"
	"time"
)

func remoteApplyMeta(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	xattrs, err := client.ListXAttrs(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err == nil && len(xattrs) > 0 {
		// TODO: restore xattrs
	}
	mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	_ = os.Chtimes(path, mt, mt)
	return nil
}

func remoteApplyMetaSymlink(_ context.Context, _ *RemoteClient, _ string, _ EntryInfo) error {
	return nil
}
