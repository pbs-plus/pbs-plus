package pxarmount

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func (fs *MutableFS) copyUp(re *ResolvedEntry) error {
	inoMu := fs.getInoLock(re.Inode)
	inoMu.Lock()
	defer inoMu.Unlock()

	if re.DataIsMut {
		return nil
	}

	// Ensure we have a journal node for this path.
	fs.ensureNode(re)
	if re.Node == nil {
		return fmt.Errorf("copyUp: could not create node for %q", re.Path)
	}

	abs := fs.mutablePath(re.Path)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return err
	}

	if re.PxarNode != nil {
		if re.PxarNode.isReg {
			if err := fs.copyUpRegularFile(re.Path, re.PxarNode); err != nil {
				return err
			}
		}
		if re.PxarNode.isSymlink {
			entry, err := fs.pxar.GetPxarEntry(re.PxarNode.inode)
			if err != nil {
				return err
			}
			if err := syscall.Symlink(entry.LinkTarget, abs); err != nil {
				return err
			}
		}
	}

	if err := fs.journal.SetHasData(re.Node.ID); err != nil {
		return fmt.Errorf("journal set has_data: %w", err)
	}
	re.DataIsMut = true
	re.Node.HasData = true

	fs.applyACLOwnership(abs)
	return nil
}

func (fs *MutableFS) copyUpRegularFile(path string, n *node) error {
	abs := fs.mutablePath(path)
	f, err := os.OpenFile(abs, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	entry, err := fs.pxar.GetPxarEntry(n.inode)
	if err != nil {
		return err
	}

	rc, err := fs.pxar.Reader().ReadFileContentReader(entry)
	if err != nil {
		return err
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	if _, err := io.CopyBuffer(f, rc, *bufp); err != nil {
		return err
	}

	mode := os.FileMode(statMode(n.mode) & 0o7777)
	if err := os.Chmod(abs, mode); err != nil {
		fs.logNonFatal("chmod", abs, err)
	}

	// Preserve extended attributes and file capabilities from the pxar
	// for write silently drops all xattrs and fcaps.
	applyPxarXattrsToFile(abs, entry)

	return nil
}

// applyPxarXattrsToFile sets extended attributes and file capabilities
// from a pxar entry onto a real file. Errors are logged but not fatal  -

// Used as a fallback when os.Rename fails during Rename operations
// (e.g. cross-device rename).
func copyRegularFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := in.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	bufp := copyBufPool.Get().(*[]byte)
	defer copyBufPool.Put(bufp)
	_, err = io.CopyBuffer(out, in, *bufp)
	return err
}

// resolve looks up a path using the inode graph, falling back to pxar.
