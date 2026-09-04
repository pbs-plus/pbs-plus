//go:build linux

package outpost

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
)

// dirFS backs one shared NFS export: each registered child filesystem is
// served as a subdirectory of a synthetic read-only tree, so many mounts can
// live under one share. Children are pre-wrapped with unique file id prefixes
// by the caller, so ids stay unique across the whole export.
type dirFS struct {
	mu       sync.RWMutex
	children map[string]billy.Filesystem
}

var (
	_ billy.Filesystem = (*dirFS)(nil)
	_ billy.Change     = (*dirFS)(nil)
)

func newDirFS() *dirFS {
	return &dirFS{children: map[string]billy.Filesystem{}}
}

// add registers fs at sub (a clean relative path; "" is not a valid sub path).
func (d *dirFS) add(sub string, fs billy.Filesystem) error {
	sub = normSub(sub)
	if sub == "" || fs == nil {
		return fmt.Errorf("shared mount needs a non-empty sub path and a filesystem")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.children[sub] = fs
	return nil
}

// remove drops the child at sub and reports the remaining child count.
func (d *dirFS) remove(sub string) int {
	sub = normSub(sub)
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.children, sub)
	return len(d.children)
}

func normSub(p string) string {
	return strings.Trim(path.Clean("/"+filepath.ToSlash(p)), "/")
}

// resolve maps p onto the child with the longest matching sub path.
func (d *dirFS) resolve(p string) (billy.Filesystem, string, bool) {
	norm := normSub(p)
	d.mu.RLock()
	defer d.mu.RUnlock()
	keys := make([]string, 0, len(d.children))
	for k := range d.children {
		if norm == k || strings.HasPrefix(norm, k+"/") {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, "", false
	}
	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	k := keys[0]
	return d.children[k], strings.TrimPrefix(strings.TrimPrefix(norm, k), "/"), true
}

// isPrefixDir reports whether p is the root or a synthetic directory level.
func (d *dirFS) isPrefixDir(p string) bool {
	norm := normSub(p)
	if norm == "" {
		return true
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for k := range d.children {
		if strings.HasPrefix(k, norm+"/") {
			return true
		}
	}
	return false
}

// under lists the sub path components directly below p.
func (d *dirFS) under(p string) []string {
	norm := normSub(p)
	d.mu.RLock()
	defer d.mu.RUnlock()
	seen := map[string]bool{}
	for k := range d.children {
		if norm != "" && !strings.HasPrefix(k, norm+"/") {
			continue
		}
		rest := k
		if norm != "" {
			rest = strings.TrimPrefix(k, norm+"/")
		}
		if rest == "" {
			continue
		}
		if name, _, ok := strings.Cut(rest, "/"); ok || true {
			seen[name] = true
		}
	}
	out := make([]string, 0, len(seen))
	for name := range seen {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

type dirEntryInfo struct {
	name string
	ino  uint64
}

func (i *dirEntryInfo) Name() string       { return i.name }
func (i *dirEntryInfo) Size() int64        { return 4096 }
func (i *dirEntryInfo) Mode() os.FileMode  { return os.ModeDir | 0o555 }
func (i *dirEntryInfo) ModTime() time.Time { return time.Time{} }
func (i *dirEntryInfo) IsDir() bool        { return true }
func (i *dirEntryInfo) Sys() any           { return nil }

// syntheticIno derives a stable 48-bit file id for synthetic directories.
func syntheticIno(p string) uint64 {
	h := fnv.New64a()
	h.Write([]byte("/" + normSub(p)))
	return h.Sum64() & ((1 << uniqueInoBits) - 1)
}

func (d *dirFS) readDir(norm string) ([]os.FileInfo, error) {
	var out []os.FileInfo
	names := map[string]bool{}
	if child, rel, ok := d.resolve(norm); ok {
		entries, err := child.ReadDir(rel)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			out = append(out, e)
			names[e.Name()] = true
		}
	} else if !d.isPrefixDir(norm) {
		return nil, os.ErrNotExist
	}
	for _, name := range d.under(norm) {
		if !names[name] {
			out = append(out, &dirEntryInfo{name: name, ino: syntheticIno(norm + "/" + name)})
		}
	}
	return out, nil
}

func (d *dirFS) ReadDir(p string) ([]os.FileInfo, error) { return d.readDir(normSub(p)) }

func (d *dirFS) Stat(p string) (os.FileInfo, error) {
	norm := normSub(p)
	if child, rel, ok := d.resolve(norm); ok {
		return child.Stat(rel)
	}
	if d.isPrefixDir(norm) {
		return &dirEntryInfo{name: path.Base(norm), ino: syntheticIno(norm)}, nil
	}
	return nil, os.ErrNotExist
}

func (d *dirFS) Lstat(p string) (os.FileInfo, error) {
	norm := normSub(p)
	if child, rel, ok := d.resolve(norm); ok {
		return child.Lstat(rel)
	}
	if d.isPrefixDir(norm) {
		return &dirEntryInfo{name: path.Base(norm), ino: syntheticIno(norm)}, nil
	}
	return nil, os.ErrNotExist
}

func (d *dirFS) Open(p string) (billy.File, error) {
	if child, rel, ok := d.resolve(p); ok {
		return child.Open(rel)
	}
	return nil, os.ErrNotExist
}

func (d *dirFS) OpenFile(p string, flag int, perm os.FileMode) (billy.File, error) {
	if child, rel, ok := d.resolve(p); ok {
		return child.OpenFile(rel, flag, perm)
	}
	return nil, os.ErrNotExist
}

func (d *dirFS) Create(p string) (billy.File, error) {
	if child, rel, ok := d.resolve(p); ok {
		return child.Create(rel)
	}
	return nil, billy.ErrNotSupported
}

func (d *dirFS) Remove(p string) error {
	if child, rel, ok := d.resolve(p); ok {
		return child.Remove(rel)
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Rename(oldpath, newpath string) error {
	from, fromRel, ok := d.resolve(oldpath)
	to, toRel, ok2 := d.resolve(newpath)
	if !ok || !ok2 || from != to {
		return errors.New("rename across shared mounts is not supported")
	}
	return from.Rename(fromRel, toRel)
}

func (d *dirFS) MkdirAll(p string, perm os.FileMode) error {
	if child, rel, ok := d.resolve(p); ok {
		return child.MkdirAll(rel, perm)
	}
	if d.isPrefixDir(p) {
		return nil
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Symlink(target, link string) error {
	if child, rel, ok := d.resolve(link); ok {
		return child.Symlink(target, rel)
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Readlink(p string) (string, error) {
	if child, rel, ok := d.resolve(p); ok {
		return child.Readlink(rel)
	}
	return "", os.ErrNotExist
}

func (d *dirFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}

// Chroot returns a static filtered view; live adds/removes do not propagate.
func (d *dirFS) Chroot(p string) (billy.Filesystem, error) {
	norm := normSub(p)
	if norm == "" {
		return d, nil
	}
	view := newDirFS()
	d.mu.RLock()
	for k, fs := range d.children {
		if after, ok := strings.CutPrefix(k, norm+"/"); ok {
			view.children[after] = fs
		}
	}
	d.mu.RUnlock()
	return view, nil
}

func (d *dirFS) Root() string               { return "/" }
func (d *dirFS) Join(elem ...string) string { return path.Join(elem...) }

func (d *dirFS) changeOf(p string) (billy.Change, billy.Filesystem, string, bool) {
	child, rel, ok := d.resolve(p)
	if !ok {
		return nil, nil, "", false
	}
	c, isChange := child.(billy.Change)
	return c, child, rel, isChange
}

func (d *dirFS) Chmod(p string, perm os.FileMode) error {
	if c, _, rel, ok := d.changeOf(p); ok {
		if c == nil {
			return billy.ErrNotSupported
		}
		return c.Chmod(rel, perm)
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Chown(p string, uid, gid int) error {
	if c, _, rel, ok := d.changeOf(p); ok {
		if c == nil {
			return billy.ErrNotSupported
		}
		return c.Chown(rel, uid, gid)
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Lchown(p string, uid, gid int) error {
	if c, _, rel, ok := d.changeOf(p); ok {
		if c == nil {
			return billy.ErrNotSupported
		}
		return c.Lchown(rel, uid, gid)
	}
	return billy.ErrNotSupported
}

func (d *dirFS) Chtimes(p string, atime, mtime time.Time) error {
	if c, _, rel, ok := d.changeOf(p); ok {
		if c == nil {
			return billy.ErrNotSupported
		}
		return c.Chtimes(rel, atime, mtime)
	}
	return billy.ErrNotSupported
}
