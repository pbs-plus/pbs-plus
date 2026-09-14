package targetplugin

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
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

	brokerToken []byte

	mu           sync.Mutex
	released     bool
	cleanup      []func() error
	eventSink    func(HostEvent) error
	agentRestore func(context.Context, HostAgentRestoreRequest) error

	closeOnce sync.Once
	closeErr  error
}

// RawStreamResponse receives canonical control metadata and its raw stream in one response.
type RawStreamResponse struct {
	Metadata any
	Handle   arpc.RawStreamHandler
}

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
	command.Cancel = func() error {
		_ = conn.Close()
		if command.Process == nil {
			return nil
		}
		if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return err
		}
		return nil
	}
	command.WaitDelay = pluginTerminateBy
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

	token := make([]byte, brokerTokenBytes)
	if _, err := rand.Read(token); err != nil {
		pipe.Close()
		<-wait
		return nil, fmt.Errorf("create plugin broker token: %w", err)
	}
	process := &Process{command: command, pipe: pipe, wait: wait, brokerToken: token}
	router := arpc.NewRouter()
	router.Handle(MethodHostEvent, process.handleHostEvent)
	router.Handle(MethodHostAgentRestore, process.handleHostAgentRestore)
	pipe.SetRouter(router)
	go func() { _ = pipe.Serve() }()

	return process, nil
}

// BrokerToken authorizes reverse host calls for this process only.
func (p *Process) BrokerToken() []byte {
	return bytes.Clone(p.brokerToken)
}

// SetEventSink receives plugin diagnostics and progress until the process is closed.
func (p *Process) SetEventSink(sink func(HostEvent) error) {
	p.mu.Lock()
	p.eventSink = sink
	p.mu.Unlock()
}

// SetAgentRestoreHandler grants this process access to the scoped agent restore broker.
func (p *Process) SetAgentRestoreHandler(handler func(context.Context, HostAgentRestoreRequest) error) {
	p.mu.Lock()
	p.agentRestore = handler
	p.mu.Unlock()
}

func (p *Process) handleHostEvent(request *arpc.Request) (arpc.Response, error) {
	var event HostEvent
	if err := UnmarshalProtocol(request.Payload, &event); err != nil {
		return arpc.Response{}, fmt.Errorf("decode host event: %w", err)
	}
	if err := event.Validate(); err != nil {
		return arpc.Response{}, fmt.Errorf("validate host event: %w", err)
	}
	if subtle.ConstantTimeCompare(event.Operation.BrokerToken, p.brokerToken) != 1 {
		return arpc.Response{}, errors.New("host event broker token is not authorized")
	}
	p.mu.Lock()
	sink := p.eventSink
	p.mu.Unlock()
	if sink != nil {
		if err := sink(event); err != nil {
			return arpc.Response{}, fmt.Errorf("handle host event: %w", err)
		}
	}
	return arpc.Response{Status: http.StatusOK}, nil
}

func (p *Process) handleHostAgentRestore(request *arpc.Request) (arpc.Response, error) {
	var restore HostAgentRestoreRequest
	if err := UnmarshalProtocol(request.Payload, &restore); err != nil {
		return arpc.Response{}, fmt.Errorf("decode host agent restore: %w", err)
	}
	if err := restore.Validate(); err != nil {
		return arpc.Response{}, fmt.Errorf("validate host agent restore: %w", err)
	}
	if subtle.ConstantTimeCompare(restore.Operation.BrokerToken, p.brokerToken) != 1 {
		return arpc.Response{}, errors.New("host agent restore broker token is not authorized")
	}
	p.mu.Lock()
	handler := p.agentRestore
	p.mu.Unlock()
	if handler == nil {
		return arpc.Response{}, errors.New("host agent restore is not available")
	}
	if err := handler(request.Context, restore); err != nil {
		return arpc.Response{}, fmt.Errorf("host agent restore: %w", err)
	}
	return arpc.Response{Status: http.StatusOK}, nil
}

func (p *Process) Describe(ctx context.Context) (Descriptor, error) {
	var descriptor Descriptor
	if err := p.callProtocol(ctx, MethodDescribe, DescribeRequest{ProtocolVersion: CurrentProtocolVersion}, &descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("describe plugin: %w", err)
	}
	return descriptor, nil
}

// Invoke performs one plugin operation without a separate description handshake.
func (p *Process) Invoke(ctx context.Context, method string, request interface{ Validate() error }, response any) error {
	if err := validateInvocationMethod(method); err != nil {
		return err
	}
	if err := p.callProtocol(ctx, method, request, response); err != nil {
		return fmt.Errorf("invoke %s: %w", method, err)
	}
	return nil
}

func (p *Process) callProtocol(ctx context.Context, method string, request, response any) error {
	if p == nil || p.pipe == nil {
		return errors.New("plugin process is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateProtocolValue("request", request); err != nil {
		return err
	}
	payload, err := MarshalProtocol(request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if raw, ok := response.(*RawStreamResponse); ok {
		if raw == nil || raw.Metadata == nil || raw.Handle == nil {
			return errors.New("raw stream response requires metadata and a handler")
		}
		return p.pipe.Call(ctx, method, payload, arpc.RawStreamDataHandler(func(data []byte, stream arpc.ARPCStream) error {
			if len(data) == 0 {
				return errors.New("plugin raw stream metadata is empty")
			}
			if err := UnmarshalProtocol(data, raw.Metadata); err != nil {
				return fmt.Errorf("decode raw stream metadata: %w", err)
			}
			if err := validateProtocolValue("response", raw.Metadata); err != nil {
				return err
			}
			return raw.Handle(stream)
		}))
	}
	var data []byte
	if err := p.pipe.Call(ctx, method, payload, &data); err != nil {
		return err
	}
	if response == nil {
		return nil
	}
	if len(data) == 0 {
		return errors.New("plugin response is empty")
	}
	if err := UnmarshalProtocol(data, response); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return validateProtocolValue("response", response)
}

func validateProtocolValue(label string, value any) error {
	if value == nil {
		return nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() == reflect.Pointer && reflected.IsNil() {
		return fmt.Errorf("plugin protocol %s is nil", label)
	}
	validator, ok := value.(interface{ Validate() error })
	if !ok {
		return nil
	}
	if err := validator.Validate(); err != nil {
		return fmt.Errorf("validate plugin protocol %s: %w", label, err)
	}
	return nil
}

func validateInvocationMethod(method string) error {
	switch method {
	case MethodPluginHealth, MethodTargetValidate, MethodTargetProbe, MethodTargetMigrate,
		MethodBackupOpen, MethodBackupCheck, MethodBackupMigrateOptions,
		MethodRestoreOpen, MethodRestoreConsume, MethodRestoreCheck, MethodRestoreMigrateOptions:
		return nil
	default:
		return fmt.Errorf("unsupported plugin invocation method %q", method)
	}
}

// AddCleanup registers host-owned release work that runs even when the plugin crashes.
func (p *Process) AddCleanup(step func() error) error {
	if step == nil {
		return errors.New("plugin cleanup step is required")
	}
	p.mu.Lock()
	if p.released {
		p.mu.Unlock()
		return step()
	}
	p.cleanup = append(p.cleanup, step)
	p.mu.Unlock()
	return nil
}

// Close closes the aRPC session, reaps the plugin process, and releases host-owned leases.
func (p *Process) Close() error {
	p.closeOnce.Do(func() {
		p.pipe.Close()
		p.closeErr = errors.Join(p.waitForExit(), p.releaseCleanup())
	})
	return p.closeErr
}

func (p *Process) releaseCleanup() error {
	p.mu.Lock()
	steps := p.cleanup
	p.cleanup = nil
	p.released = true
	p.mu.Unlock()

	var err error
	for _, step := range slices.Backward(steps) {
		err = errors.Join(err, step())
	}
	return err
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
