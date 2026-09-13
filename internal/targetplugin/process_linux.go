package targetplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"golang.org/x/sys/unix"
)

const (
	pluginFD          = 3
	pluginExitGrace   = 2 * time.Second
	pluginTerminateBy = 2 * time.Second
)

// Process is one supervised target plugin subprocess.
type Process struct {
	command *exec.Cmd
	pipe    *arpc.StreamPipe
	wait    <-chan error

	closeOnce sync.Once
	closeErr  error
}

// Start launches a plugin using an inherited Unix socket and no shell.
func Start(ctx context.Context, executable string, args ...string) (*Process, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !filepath.IsAbs(executable) {
		return nil, errors.New("plugin executable path must be absolute")
	}
	info, err := os.Stat(executable)
	if err != nil {
		return nil, fmt.Errorf("stat plugin executable: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, errors.New("plugin executable must be an executable regular file")
	}

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("create plugin socket pair: %w", err)
	}
	parentFile := os.NewFile(uintptr(fds[0]), "pbs-plus-plugin-host")
	childFile := os.NewFile(uintptr(fds[1]), "pbs-plus-plugin-child")
	if parentFile == nil || childFile == nil {
		if parentFile != nil {
			_ = parentFile.Close()
		}
		if childFile != nil {
			_ = childFile.Close()
		}
		return nil, errors.New("open plugin socket pair")
	}

	conn, err := net.FileConn(parentFile)
	_ = parentFile.Close()
	if err != nil {
		_ = childFile.Close()
		return nil, fmt.Errorf("open plugin host connection: %w", err)
	}

	command := exec.CommandContext(ctx, executable, args...)
	command.ExtraFiles = []*os.File{childFile}
	command.Env = pluginEnvironment()
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}
	if err := command.Start(); err != nil {
		_ = childFile.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("start plugin: %w", err)
	}
	_ = childFile.Close()

	pipe, err := arpc.NewClientPipe(ctx, conn)
	if err != nil {
		_ = command.Process.Kill()
		_ = command.Wait()
		return nil, fmt.Errorf("connect plugin: %w", err)
	}

	wait := make(chan error, 1)
	go func() {
		wait <- command.Wait()
		close(wait)
	}()

	return &Process{command: command, pipe: pipe, wait: wait}, nil
}

// Describe retrieves and validates the plugin identity before other calls are allowed.
func (p *Process) Describe(ctx context.Context) (Descriptor, error) {
	var descriptor Descriptor
	request := DescribeRequest{ProtocolVersion: CurrentProtocolVersion}
	if err := p.pipe.Call(ctx, MethodDescribe, request, &descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("describe plugin: %w", err)
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, fmt.Errorf("validate plugin descriptor: %w", err)
	}
	return descriptor, nil
}

// Close closes the aRPC session and reaps the plugin process.
func (p *Process) Close() error {
	p.closeOnce.Do(func() {
		p.pipe.Close()
		p.closeErr = p.waitForExit()
	})
	return p.closeErr
}

func (p *Process) waitForExit() error {
	select {
	case err := <-p.wait:
		return err
	case <-time.After(pluginExitGrace):
	}

	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("terminate plugin: %w", err)
	}

	select {
	case err := <-p.wait:
		return err
	case <-time.After(pluginTerminateBy):
	}

	if err := p.command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill plugin: %w", err)
	}
	<-p.wait
	return errors.New("plugin did not exit after SIGTERM")
}

func pluginEnvironment() []string {
	environment := []string{SocketFDEnv + "=" + strconv.Itoa(pluginFD)}
	for _, key := range []string{"HOME", "LANG", "LC_ALL", "PATH", "TZ"} {
		if value, ok := os.LookupEnv(key); ok {
			environment = append(environment, key+"="+value)
		}
	}
	return environment
}
