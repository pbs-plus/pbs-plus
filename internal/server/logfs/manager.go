//go:build linux

package logfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

const (
	// backingSuffix is appended to the original directory to create
	// the backing store path.
	backingSuffix = ".logfs"
)

// MountManager handles the full FUSE mount lifecycle: moving the real
// directory aside, mounting, and restoring on shutdown or crash.
type MountManager struct {
	mu          sync.Mutex
	originalDir string
	backingDir  string
	server      *fuse.Server
	bus         *EventBus
	mounted     bool
}

// NewMountManager creates a MountManager for the given directory.
func NewMountManager(dir string) *MountManager {
	return &MountManager{
		originalDir: dir,
		backingDir:  dir + backingSuffix,
		bus:         NewEventBus(),
	}
}

// EventBus returns the EventBus shared by this mount.
func (m *MountManager) EventBus() *EventBus {
	return m.bus
}

// Mount prepares the backing directory and mounts the FUSE passthrough
// filesystem. It handles crash recovery by detecting leftover state
// from a previous run.
func (m *MountManager) Mount() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mounted {
		return nil
	}

	backingExists := dirExists(m.backingDir)
	originalIsMount := isFUSEMount(m.originalDir)

	switch {
	case backingExists && originalIsMount:
		syslog.L.Warn().
			WithMessage("FUSE mount already active, unmounting stale mount").
			WithField("path", m.originalDir).
			Write()
		_ = m.server.Unmount()

	case backingExists && !originalIsMount:
		syslog.L.Info().
			WithMessage("recovering from previous FUSE crash, reusing backing dir").
			WithField("backing", m.backingDir).
			Write()

	case !backingExists:
		if dirExists(m.originalDir) {
			if err := os.Rename(m.originalDir, m.backingDir); err != nil {
				return fmt.Errorf("logfs: failed to move %s to %s: %w",
					m.originalDir, m.backingDir, err)
			}
			syslog.L.Info().
				WithMessage("moved log directory to backing path").
				WithField("original", m.originalDir).
				WithField("backing", m.backingDir).
				Write()
		} else {
			if err := os.MkdirAll(m.backingDir, 0755); err != nil {
				return fmt.Errorf("logfs: failed to create backing dir %s: %w",
					m.backingDir, err)
			}
		}
	}

	if err := os.MkdirAll(m.originalDir, 0755); err != nil {
		return fmt.Errorf("logfs: failed to create mount point %s: %w",
			m.originalDir, err)
	}

	server, err := Mount(m.backingDir, m.originalDir, m.bus, nil)
	if err != nil {
		m.restoreLocked()
		return fmt.Errorf("logfs: FUSE mount failed: %w", err)
	}

	m.server = server
	m.mounted = true

	LogFS = &MountInfo{
		BackingDir: m.backingDir,
		MountPoint: m.originalDir,
	}

	syslog.L.Info().
		WithMessage("FUSE log passthrough mounted").
		WithField("backing", m.backingDir).
		WithField("mount", m.originalDir).
		Write()

	return nil
}

// Unmount gracefully unmounts the FUSE filesystem and restores the
// original directory layout.
func (m *MountManager) Unmount() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.mounted {
		return
	}

	syslog.L.Info().
		WithMessage("unmounting FUSE log passthrough").
		Write()

	// Try syscall unmount first, then server.Unmount as fallback.
	if err := syscall.Unmount(m.originalDir, 0); err != nil {
		syslog.L.Error(err).
			WithMessage("syscall unmount failed, trying server unmount").
			Write()
		if m.server != nil {
			_ = m.server.Unmount()
		}
	}

	m.restoreLocked()
	m.mounted = false
	m.server = nil
	LogFS = nil

	syslog.L.Info().
		WithMessage("FUSE log passthrough unmounted, original directory restored").
		Write()
}

// Serve starts the FUSE event loop. This blocks until the filesystem
// is unmounted. Call in a goroutine.
func (m *MountManager) Serve() {
	m.mu.Lock()
	server := m.server
	m.mu.Unlock()

	if server != nil {
		server.Wait()
	}
}

// restoreLocked moves the backing directory back to the original path.
// Caller must hold m.mu.
func (m *MountManager) restoreLocked() {
	if !dirExists(m.backingDir) {
		return
	}

	_ = os.Remove(m.originalDir)

	if err := os.Rename(m.backingDir, m.originalDir); err != nil {
		syslog.L.Error(err).
			WithMessage("failed to restore original log directory").
			WithField("backing", m.backingDir).
			WithField("original", m.originalDir).
			Write()
		return
	}

	syslog.L.Info().
		WithMessage("restored original log directory").
		WithField("path", m.originalDir).
		Write()
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

func isFUSEMount(path string) bool {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return false
	}
	parent := filepath.Dir(path)
	if parent == path {
		return false
	}
	var parentSt syscall.Stat_t
	if err := syscall.Stat(parent, &parentSt); err != nil {
		return false
	}
	return st.Dev != parentSt.Dev
}
