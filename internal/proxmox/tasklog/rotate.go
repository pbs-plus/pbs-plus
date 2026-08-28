//go:build linux

package tasklog

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"log/slog"
)

// RotateArchive is PBS's rotate_task_log_archive: shift the archive once
// it exceeds sizeThreshold (compressing the shifted file when compress is
// set), keep at most maxFiles rotated variants, or with maxDays delete
// variants whose newest task predates the cutoff.
func RotateArchive(sizeThreshold int64, compress bool, maxFiles, maxDays int) (bool, error) {
	lock, err := lockTaskList(true)
	if err != nil {
		return false, err
	}
	defer lock.Close()

	info, err := os.Stat(archivePath)
	rotated := false
	if err == nil && info.Size() >= sizeThreshold {
		if err := shiftArchive(compress); err != nil {
			return false, err
		}
		rotated = true
	}

	files := archiveFiles()
	switch {
	case maxDays > 0:
		cutoff := time.Now().Unix() - int64(maxDays)*24*60*60
		for _, path := range files[1:] {
			newest, err := newestArchiveTaskTime(path)
			if err != nil {
				continue
			}
			if newest < cutoff {
				if err := os.Remove(path); err != nil {
					slog.Error("tasklog: remove old archive", "error", err, "path", path)
				}
			}
		}
	case maxFiles > 0 && len(files) > maxFiles:
		for _, path := range files[maxFiles:] {
			if err := os.Remove(path); err != nil {
				slog.Error("tasklog: remove excess archive", "error", err, "path", path)
			}
		}
	}

	return rotated, nil
}

// shiftArchive moves archive.N(.gz) -> archive.(N+1)(.gz) from oldest to
// newest, then rotates the live archive out, matching PBS's
// LogRotate::rotate.
func shiftArchive(compress bool) error {
	for n := 19; n >= 1; n-- {
		for _, ext := range []string{".gz", ""} {
			src := fmt.Sprintf("%s.%d%s", archivePath, n, ext)
			if _, err := os.Stat(src); err != nil {
				continue
			}
			dst := fmt.Sprintf("%s.%d%s", archivePath, n+1, ext)
			if err := os.Rename(src, dst); err != nil {
				return fmt.Errorf("tasklog: rotate archive: %w", err)
			}
		}
	}

	if compress {
		if err := compressFile(archivePath, archivePath+".1.gz"); err != nil {
			return err
		}
		return os.Remove(archivePath)
	}
	if err := os.Rename(archivePath, archivePath+".1"); err != nil {
		return fmt.Errorf("tasklog: rotate archive: %w", err)
	}
	return nil
}

func compressFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("tasklog: open for compress: %w", err)
	}
	defer func() {
		if cerr := in.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0660)
	if err != nil {
		return fmt.Errorf("tasklog: create compressed archive: %w", err)
	}

	gzw := gzip.NewWriter(out)
	_, copyErr := io.Copy(gzw, in)
	if copyErr == nil {
		copyErr = gzw.Close()
	} else if cerr := gzw.Close(); cerr != nil {
		slog.Error(cerr.Error())
	}
	if cerr := out.Close(); cerr != nil && copyErr == nil {
		copyErr = cerr
	}
	if copyErr != nil {
		return fmt.Errorf("tasklog: compress archive: %w", copyErr)
	}
	return nil
}

// CleanupOldTasks is PBS's cleanup_old_tasks: remove task logs older
// than the oldest entry still present in the oldest archive file.
func CleanupOldTasks() error {
	lock, err := lockTaskList(true)
	if err != nil {
		return err
	}
	defer lock.Close()

	files := archiveFiles()
	if len(files) == 0 {
		return nil
	}

	var cutoff int64
	found := false
	list, err := readTaskFileAny(files[len(files)-1])
	if err != nil {
		return nil
	}
	for _, info := range list {
		if info.State != nil && (!found || info.State.EndTime < cutoff) {
			cutoff = info.State.EndTime
			found = true
		}
	}
	if !found {
		return nil
	}

	entries, err := os.ReadDir(taskDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "UPID:") {
			continue
		}
		task, perr := proxmox.ParseUPID(name)
		if perr != nil {
			continue
		}
		if task.StartTime < cutoff {
			if err := os.Remove(filepath.Join(taskDir, name)); err != nil {
				slog.Error("tasklog: remove old task log", "error", err, "path", name)
			}
		}
	}
	return nil
}

func newestArchiveTaskTime(path string) (int64, error) {
	list, err := readTaskFileAny(path)
	if err != nil {
		return 0, err
	}
	var newest int64
	for _, info := range list {
		if info.State != nil && info.State.EndTime > newest {
			newest = info.State.EndTime
		}
	}
	if newest == 0 {
		return 0, fmt.Errorf("tasklog: no dated entries in %s", path)
	}
	return newest, nil
}
