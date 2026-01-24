//go:build linux

package pxar

import (
	"context"
	"os"
	"path/filepath"
	"sync"
)

func localRestoreDir(
	ctx context.Context,
	pr *PxarReader,
	dst string,
	dirEntry *EntryInfo,
	jobs chan<- localJob,
	wg *sync.WaitGroup,
	errChan chan error,
) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}

	entries, err := pr.ReadDir(dirEntry.EntryRangeEnd)
	if err != nil {
		return err
	}

	for _, e := range entries {
		target := filepath.Join(dst, e.Name())

		if e.IsDir() || e.IsFile() || e.IsSymlink() {
			wg.Add(1)
			select {
			case jobs <- localJob{dest: target, info: e}:
			case <-ctx.Done():
				wg.Done()
				return ctx.Err()
			}
		} else {
			if err := restoreSpecialFile(pr, target, e); err != nil {
				errChan <- err
			}
		}
	}

	df, err := os.Open(dst)
	if err != nil {
		return err
	}
	return localApplyMeta(pr, df, dirEntry)
}
