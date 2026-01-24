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
			job := localJob{dest: target, info: e}
			select {
			case jobs <- job:
				// Success, sent without blocking
			default:
				// Channel is full, spawn goroutine to avoid deadlock.
				go func(j localJob) {
					select {
					case jobs <- j:
					case <-ctx.Done():
						wg.Done()
					}
				}(job)
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
