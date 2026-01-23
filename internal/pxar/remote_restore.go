package pxar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

func RemoteRestore(ctx context.Context, client *RemoteClient, sources []string, destDir string) error {
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}

	for _, source := range sources {
		sourceAttr, err := client.LookupByPath(ctx, source)
		if err != nil {
			_ = client.SendError(ctx, err)
			continue
		}
		if sourceAttr.IsDir() {
			err = remoteRestoreDir(ctx, client, destDir, sourceAttr)
			if err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		} else {
			path := filepath.Join(destDir, sourceAttr.Name())
			err = remoteRestoreFile(ctx, client, path, sourceAttr)
			if err != nil {
				_ = client.SendError(ctx, err)
				continue
			}
		}
	}
	return nil
}

func remoteRestoreFile(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	mode := os.FileMode(e.Mode & 0777)
	if runtime.GOOS == "windows" {
		mode = 0666
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return fmt.Errorf("create file %q: %w", path, err)
	}
	defer f.Close()

	if e.ContentRange != nil {
		start, end := e.ContentRange[0], e.ContentRange[1]
		var off uint64
		buf := make([]byte, 1<<20)

		for off < e.Size {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			size := uint(len(buf))
			remain := e.Size - off
			if remain < uint64(size) {
				size = uint(remain)
			}
			data, err := client.Read(ctx, start, end, off, size)
			if err != nil {
				return fmt.Errorf("read content %q: %w", path, err)
			}
			if len(data) == 0 {
				break
			}
			if _, err := f.Write(data); err != nil {
				return fmt.Errorf("write content %q: %w", path, err)
			}
			off += uint64(len(data))
		}
	}

	return remoteApplyMeta(ctx, client, path, e)
}

func remoteRestoreSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	target, err := client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}
	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}
	return remoteApplyMetaSymlink(ctx, client, path, e)
}
