//go:build linux

package vfs

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"

	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/crypto"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

type MemcachedConfig struct {
	// Path to the Unix domain socket to bind (e.g., /tmp/myapp/memcached.sock).
	SocketPath string

	// Memory limit in MB. Special values:
	//   0: "practically unlimited" -> use a very large value (1_000_000 MB)
	//  -1: do NOT pass -m (use memcached default, usually 64 MB)
	MemoryMB int

	// Max simultaneous connections. Special values:
	//   0: do NOT pass -c (use memcached default)
	MaxConnections int

	//    0: do NOT pass -t (use memcached default)
	Threads int

	MemcachedBinary string

	// Additional args to append, if you need extra flags.
	ExtraArgs []string

	StartupTimeout time.Duration
}

// StartMemcachedOnUnixSocket starts a memcached server bound to the given Unix
// socket and returns a stop function that terminates the server and cleans up.
func StartMemcachedOnUnixSocket(ctx context.Context, cfg MemcachedConfig) (stop func() error, err error) {
	if strings.TrimSpace(cfg.SocketPath) == "" {
		return nil, errors.New("SocketPath is required")
	}

	bin := cfg.MemcachedBinary
	if bin == "" {
		bin = "memcached"
	}

	timeout := cfg.StartupTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	if err := os.MkdirAll(filepath.Dir(cfg.SocketPath), 0o755); err != nil {
		return nil, fmt.Errorf("create socket dir: %w", err)
	}

	if _, statErr := os.Stat(cfg.SocketPath); statErr == nil {
		if socketIsLive(cfg.SocketPath) {
			return nil, fmt.Errorf("socket %s already serving a live memcached", cfg.SocketPath)
		}
		if err := os.Remove(cfg.SocketPath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}

	args := []string{
		"-s", cfg.SocketPath,
		"-a", "0700",
		// Disable TCP to keep it Unix-socket only.
		"-p", "0",
		"-u", "root",
	}

	sysMem, err := conf.GetSysMem()
	if err != nil {
		return nil, fmt.Errorf("reading system memory: %w", err)
	}

	switch {
	case cfg.MemoryMB > 0:
		args = append(args, "-m", fmt.Sprintf("%d", cfg.MemoryMB))
	case cfg.MemoryMB == 0:
		args = append(args, "-m", strconv.Itoa(int(sysMem.Available/(1024*1024))))
	case cfg.MemoryMB == -1:
		// do not pass -m; memcached default will apply
	default:
		args = append(args, "-m", "64")
	}

	switch {
	case cfg.MaxConnections > 0:
		args = append(args, "-c", fmt.Sprintf("%d", cfg.MaxConnections))
	case cfg.MaxConnections == -1:
		args = append(args, "-c", "1000000")
	case cfg.MaxConnections == 0:
		// do not pass -c; use memcached default
	}

	switch {
	case cfg.Threads > 0:
		args = append(args, "-t", fmt.Sprintf("%d", cfg.Threads))
	case cfg.Threads == -1:
		args = append(args, "-t", fmt.Sprintf("%d", runtime.NumCPU()))
	case cfg.Threads == 0:
		// do not pass -t; memcached default
	}

	args = append(args, cfg.ExtraArgs...)

	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting memcached: %w", err)
	}

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := waitForUnixSocket(readyCtx, cfg.SocketPath); err != nil {
		if err := terminateProcessGroup(cmd.Process); err != nil {
			log.Error(err, "")
		}
		if err := cmd.Wait(); err != nil {
			log.Error(err, "")
		}
		if err := os.Remove(cfg.SocketPath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
		return nil, fmt.Errorf("memcached did not become ready: %w", err)
	}

	var stopOnce sync.Once
	stopFn := func() error {
		stopOnce.Do(func() {
			if err := terminateProcessGroup(cmd.Process); err != nil {
				log.Error(err, "")
			}
			var exitErr *exec.ExitError
			if err := cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
				log.Error(err, "")
			}
			if err := os.Remove(cfg.SocketPath); err != nil && !os.IsNotExist(err) {
				log.Error(err, "")
			}
		})
		return nil
	}

	go func() {
		<-ctx.Done()
		if err := stopFn(); err != nil {
			log.Error(err, "")
		}
	}()

	return stopFn, nil
}

var memcachedInstance atomic.Uint64

// NextMemcachedInstance returns a process-unique socket suffix. Keying the
// socket on the backup ID alone lets a second filesystem for the same backup
// steal the path and orphan the first instance.
func NextMemcachedInstance() uint64 {
	return memcachedInstance.Add(1)
}

// socketIsLive reports whether something still accepts on path.
func socketIsLive(path string) bool {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return false
	}
	if err := conn.Close(); err != nil {
		log.Error(err, "")
	}
	return true
}

// ReapStaleMemcachedSockets removes socket files left behind by instances that
// died without running their stop function, so the directory does not grow
// without bound across restarts.
func ReapStaleMemcachedSockets() {
	entries, err := os.ReadDir(conf.MemcachedSocketPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Error(err, "failed to scan memcached socket directory")
		}
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sock") {
			continue
		}
		path := filepath.Join(conf.MemcachedSocketPath, entry.Name())
		if socketIsLive(path) {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Error(err, "failed to remove stale memcached socket", "path", path)
		}
	}
}

func waitForUnixSocket(ctx context.Context, path string) error {
	d := net.Dialer{}
	for {
		conn, err := d.DialContext(ctx, "unix", path)
		if err == nil {
			if err := conn.Close(); err != nil {
				log.Error(err, "")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func terminateProcessGroup(p *os.Process) error {
	if p == nil {
		return nil
	}
	pgid, err := syscall.Getpgid(p.Pid)
	if err == nil {
		if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil {
			log.Error(err, "")
		}
		time.Sleep(250 * time.Millisecond)
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil {
			log.Error(err, "")
		}
		return nil
	}
	if err := p.Signal(syscall.SIGTERM); err != nil {
		log.Error(err, "")
	}
	time.Sleep(250 * time.Millisecond)
	if err := p.Kill(); err != nil {
		log.Error(err, "")
	}
	return nil
}

const memcachedKeyLimit = 240

func Key(originalKey string) string {
	if originalKey == "" {
		return "/"
	}

	if len(originalKey) > memcachedKeyLimit || !isSafe(originalKey) {
		return hashString(originalKey)
	}

	return originalKey
}

func hashString(s string) string {
	return crypto.SHA256Hex([]byte(s))
}

func isSafe(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') &&
			c != '-' && c != '.' && c != '_' && c != '*' {
			return false
		}
	}
	return true
}
