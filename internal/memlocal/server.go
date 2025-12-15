//go:build linux

package memlocal

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/minio/highwayhash"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

// MemcachedConfig controls how the embedded memcached is launched.
type MemcachedConfig struct {
	// Path to the Unix domain socket to bind (e.g., /tmp/myapp/memcached.sock).
	SocketPath string

	// Memory limit in MB. Special values:
	//   >0: use this exact value for -m
	//   0: "practically unlimited" -> use a very large value (1_000_000 MB)
	//  -1: do NOT pass -m (use memcached default, usually 64 MB)
	MemoryMB int

	// Max simultaneous connections. Special values:
	//   >0: use exact value for -c
	//   0: do NOT pass -c (use memcached default)
	//  -1: pass a very large value (1_000_000)
	MaxConnections int

	// Threads for worker threads.
	//   >0: use exact value for -t
	//    0: do NOT pass -t (use memcached default)
	//   -1: set to runtime.NumCPU()
	Threads int

	// Optional: path to memcached binary. If empty, uses "memcached" in PATH.
	MemcachedBinary string

	// Additional args to append, if you need extra flags.
	ExtraArgs []string

	// Optional startup timeout for readiness probing.
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
		_ = os.Remove(cfg.SocketPath)
	}

	args := []string{
		"-s", cfg.SocketPath,
		"-a", "0700",
		// Disable TCP to keep it Unix-socket only.
		"-p", "0",
		"-u", "root",
	}

	sysMem, err := utils.GetSysMem()

	// Memory handling
	switch {
	case cfg.MemoryMB > 0:
		args = append(args, "-m", fmt.Sprintf("%d", cfg.MemoryMB))
	case cfg.MemoryMB == 0:
		args = append(args, "-m", strconv.Itoa(int(sysMem.Available/(1024*1024))))
	case cfg.MemoryMB == -1:
		// do not pass -m; memcached default will apply
	default:
		// If not set, pick a reasonable default (64 MB) to keep behavior stable.
		args = append(args, "-m", "64")
	}

	// Connections handling
	switch {
	case cfg.MaxConnections > 0:
		args = append(args, "-c", fmt.Sprintf("%d", cfg.MaxConnections))
	case cfg.MaxConnections == -1:
		args = append(args, "-c", "1000000")
	case cfg.MaxConnections == 0:
		// do not pass -c; use memcached default
	}

	// Threads handling
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
		_ = terminateProcessGroup(cmd.Process)
		_ = cmd.Wait()
		_ = os.Remove(cfg.SocketPath)
		return nil, fmt.Errorf("memcached did not become ready: %w", err)
	}

	stopped := false
	stopFn := func() error {
		if stopped {
			return nil
		}
		stopped = true
		_ = terminateProcessGroup(cmd.Process)
		_ = cmd.Wait()
		_ = os.Remove(cfg.SocketPath)
		return nil
	}

	go func() {
		<-ctx.Done()
		_ = stopFn()
	}()

	return stopFn, nil
}

func waitForUnixSocket(ctx context.Context, path string) error {
	d := net.Dialer{}
	for {
		conn, err := d.DialContext(ctx, "unix", path)
		if err == nil {
			_ = conn.Close()
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
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		time.Sleep(250 * time.Millisecond)
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		return nil
	}
	_ = p.Signal(syscall.SIGTERM)
	time.Sleep(250 * time.Millisecond)
	_ = p.Kill()
	return nil
}

var hashKey []byte

func init() {
	hashKey = make([]byte, 32)
	n, err := rand.Read(hashKey)
	if err != nil {
		log.Fatalf("Error reading random bytes for key: %v", err)
	}
	if n != 32 {
		log.Fatalf("Expected to read 32 bytes for key, got %d", n)
	}
}

func Key(raw string) string {
	if raw == "" {
		raw = "/"
	}

	byteRaw := unsafe.Slice(unsafe.StringData(raw), len(raw))
	h, err := highwayhash.New(hashKey)
	if err != nil {
		syslog.L.Error(err).WithField("raw", raw).Write()
		return raw
	}
	h.Write(byteRaw)
	sum := h.Sum(nil)

	return string(sum)
}
