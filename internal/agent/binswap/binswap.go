package binswap

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	stagedSuffix   = ".staged"
	previousSuffix = ".previous"
	pendingSuffix  = ".update-pending"
	swapTempSuffix = ".swapping"
	badSuffix      = ".bad"

	MaxBootAttempts = 3

	CommitGrace = 60 * time.Second
)

var ErrNotStaged = errors.New("binswap: no staged binary to swap")

type Manager struct {
	exePath string
}

type pendingUpdate struct {
	Version      string    `json:"version"`
	PreviousPath string    `json:"previous_path"`
	Attempts     int       `json:"attempts"`
	StartedAt    time.Time `json:"started_at"`
}

func New(exePath string) *Manager {
	return &Manager{exePath: exePath}
}

func NewFromExecutable() (*Manager, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return &Manager{exePath: exe}, nil
}

func (m *Manager) Stage(data []byte) error {
	return atomicWriteFile(m.stagedPath(), data, 0o755)
}

func (m *Manager) SnapshotCurrent() error {
	return copyFile(m.previousPath(), m.exePath, 0o755)
}

func (m *Manager) MarkPending(version string) error {
	p := &pendingUpdate{
		Version:      version,
		PreviousPath: m.previousPath(),
		StartedAt:    time.Now().UTC(),
	}
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return atomicWriteFile(m.pendingPath(), data, 0o644)
}

func (m *Manager) Swap() error {
	staged := m.stagedPath()
	if _, err := os.Stat(staged); err != nil {
		return ErrNotStaged
	}

	tmpOld := m.swapTempPath()
	_ = os.Remove(tmpOld)

	if err := os.Rename(m.exePath, tmpOld); err != nil {
		return err
	}
	if err := os.Rename(staged, m.exePath); err != nil {
		_ = os.Rename(tmpOld, m.exePath)
		return err
	}

	_ = os.Remove(tmpOld)
	return nil
}

func (m *Manager) Rollback() error {
	prev := m.previousPath()
	if _, err := os.Stat(prev); err != nil {
		return err
	}

	badPath := m.badPath()
	_ = os.Remove(badPath)

	if err := os.Rename(m.exePath, badPath); err != nil {
		return err
	}
	if err := os.Rename(prev, m.exePath); err != nil {
		_ = os.Rename(badPath, m.exePath)
		return err
	}

	_ = os.Remove(badPath)
	return nil
}

func (m *Manager) PreviousExists() bool {
	_, err := os.Stat(m.previousPath())
	return err == nil
}

func (m *Manager) HasPending() bool {
	_, ok := m.readPending()
	return ok
}

func (m *Manager) CheckPending() (pending bool, rollback bool) {
	p, ok := m.readPending()
	if !ok {
		return false, false
	}

	p.Attempts++
	_ = m.writePending(p)

	if p.Attempts > MaxBootAttempts {
		if err := m.Rollback(); err != nil {
			m.ClearPending()
			return false, false
		}
		m.ClearPending()
		return false, true
	}

	return true, false
}

func (m *Manager) Commit() {
	m.ClearPending()
}

func (m *Manager) ClearPending() {
	_ = os.Remove(m.pendingPath())
}

func (m *Manager) Prune() {
	for _, suffix := range []string{stagedSuffix, swapTempSuffix, badSuffix} {
		_ = os.Remove(m.exePath + suffix)
	}
}

func (m *Manager) stagedPath() string   { return m.exePath + stagedSuffix }
func (m *Manager) previousPath() string { return m.exePath + previousSuffix }
func (m *Manager) pendingPath() string  { return m.exePath + pendingSuffix }
func (m *Manager) swapTempPath() string { return m.exePath + swapTempSuffix }
func (m *Manager) badPath() string      { return m.exePath + badSuffix }

func (m *Manager) readPending() (*pendingUpdate, bool) {
	data, err := os.ReadFile(m.pendingPath())
	if err != nil {
		return nil, false
	}
	var p pendingUpdate
	if err := json.Unmarshal(data, &p); err != nil {
		_ = os.Remove(m.pendingPath())
		return nil, false
	}
	return &p, true
}

func (m *Manager) writePending(p *pendingUpdate) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return atomicWriteFile(m.pendingPath(), data, 0o644)
}

func (m *Manager) PendingVersion() string {
	if p, ok := m.readPending(); ok {
		return p.Version
	}
	return ""
}

func atomicWriteFile(dst string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(dst)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-binswap-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}

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
