package pxar

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"
)

func RestoreToLocal(ctx context.Context, rootDir string, client *RemoteClient) error {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return fmt.Errorf("mkdir root: %w", err)
	}
	root, err := client.GetRoot(ctx)
	if err != nil {
		return err
	}
	if !root.IsDir() {
		return fmt.Errorf("archive root is not a directory")
	}
	return restoreDir(ctx, client, rootDir, root)
}

func restoreFile(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
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

	return applyMeta(path, e)
}

func restoreSymlink(ctx context.Context, client *RemoteClient, path string, e EntryInfo) error {
	target, err := client.ReadLink(ctx, e.EntryRangeStart, e.EntryRangeEnd)
	if err != nil {
		return fmt.Errorf("readlink data %q: %w", path, err)
	}
	if err := os.Symlink(string(target), path); err != nil {
		return fmt.Errorf("symlink %q: %w", path, err)
	}
	return applyMetaSymlink(path, e)
}

func applyMeta(path string, e EntryInfo) error {
	if runtime.GOOS != "windows" {
		_ = os.Chmod(path, os.FileMode(e.Mode&0777))
		_ = os.Chown(path, int(e.UID), int(e.GID))
	}
	mt := time.Unix(e.MtimeSecs, int64(e.MtimeNsecs))
	_ = os.Chtimes(path, mt, mt)
	return nil
}

func applyMetaSymlink(path string, e EntryInfo) error {
	if runtime.GOOS != "windows" {
		_ = os.Lchown(path, int(e.UID), int(e.GID))
	}
	return nil
}
