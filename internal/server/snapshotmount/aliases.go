//go:build linux

package snapshotmount

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pbs-plus/pbs-plus/internal/server/jobs"
	"github.com/pbs-plus/pbs-plus/internal/server/outpost"
)

func validateAliases(in jobs.SnapshotMountInput) error {
	if len(in.Aliases) == 0 {
		return nil
	}
	if len(in.Aliases) > 63 || in.SubPath == "" || in.Mode == ModeRW {
		return errors.New("aliases require a read-only batch mount and at most 64 paths")
	}
	paths := append([]string{in.SubPath}, in.Aliases...)
	for i, name := range paths {
		if name == "" {
			return errors.New("empty alias path")
		}
		if err := ValidateSubPath(name); err != nil {
			return err
		}
		for _, other := range paths[:i] {
			if name == other || strings.HasPrefix(name, other+"/") || strings.HasPrefix(other, name+"/") {
				return errors.New("alias paths overlap")
			}
		}
	}
	return nil
}

func sessionPaths(s Session) []string {
	return append([]string{s.SubPath}, s.Aliases...)
}

func attachSessionPaths(s Session, share string, att outpost.Attachment) error {
	closeStack := func() {}
	if att.Release != nil {
		closeStack = sync.OnceFunc(att.Release)
	}
	if s.SubPath == "" {
		att.Release = closeStack
		if err := outpost.Attach(s.Outpost, att); err != nil {
			closeStack()
			return err
		}
		return nil
	}
	paths := sessionPaths(s)
	var refs atomic.Int64
	refs.Store(int64(len(paths)))
	att.Release = func() {
		if refs.Add(-1) == 0 {
			closeStack()
		}
	}
	for i, sub := range paths {
		if err := outpost.AttachSub(s.Outpost, share, sub, att); err != nil {
			for _, added := range paths[:i] {
				outpost.DetachSub(s.Outpost, share, added)
			}
			closeStack()
			return err
		}
	}
	return nil
}

func detachSessionPaths(s Session) {
	share := ShareName(s)
	if s.SubPath == "" {
		outpost.Detach(s.Outpost, share)
		return
	}
	for _, sub := range sessionPaths(s) {
		outpost.DetachSub(s.Outpost, share, sub)
	}
	if !sharedShareInUse(s) {
		outpost.Detach(s.Outpost, share)
	}
}

func localAliasRoot(s Session) (string, error) {
	suffix := string(os.PathSeparator) + s.SubPath
	if s.SubPath == "" || !strings.HasSuffix(s.MountPoint, suffix) {
		return "", errors.New("alias mount is outside its batch root")
	}
	return strings.TrimSuffix(s.MountPoint, suffix), nil
}

func localAliases(s Session, remove bool) (err error) {
	if len(s.Aliases) == 0 {
		return nil
	}
	base, err := localAliasRoot(s)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(base)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()
	for _, alias := range s.Aliases {
		target, err := filepath.Rel(filepath.Dir(alias), s.SubPath)
		if err != nil {
			return err
		}
		existing, err := root.Readlink(alias)
		if err == nil && existing == target {
			if remove {
				if err := root.Remove(alias); err != nil {
					return err
				}
			}
			continue
		}
		if remove {
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("alias %q is not owned by this mount", alias)
			}
			continue
		}
		if err := root.MkdirAll(filepath.Dir(alias), 0o755); err != nil {
			return err
		}
		if err := root.Symlink(target, alias); err != nil {
			return fmt.Errorf("create mount alias %q: %w", alias, err)
		}
	}
	return nil
}
