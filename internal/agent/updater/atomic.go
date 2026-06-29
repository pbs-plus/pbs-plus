package updater

import (
	"io"
	"os"
	"path/filepath"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func atomicWriteFile(dst string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-update-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			if err := os.Remove(tmpName); err != nil && !os.IsNotExist(err) {
				log.Error(err, "updater: failed to remove temp file")
			}
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}

	_ = os.Remove(dst)
	if err := os.Rename(tmpName, dst); err != nil {
		return err
	}

	committed = true
	return nil
}

func copyFile(dst, src string, perm os.FileMode) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	return atomicWriteFile(dst, data, perm)
}
