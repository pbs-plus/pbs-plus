package targetplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

func TestProcessDescribe(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	process, err := Start(ctx, executable, "-test.run=^TestPluginProcessHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	descriptor, err := process.Describe(ctx)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if descriptor.PluginID != "org.pbs-plus.test" {
		t.Fatalf("PluginID = %q, want org.pbs-plus.test", descriptor.PluginID)
	}
	if len(descriptor.TargetTypes) != 1 || descriptor.TargetTypes[0] != "test" {
		t.Fatalf("TargetTypes = %v, want [test]", descriptor.TargetTypes)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessCrash(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	process, err := Start(t.Context(), executable, "-test.run=^TestPluginCrashHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := process.Describe(t.Context()); err == nil {
		t.Fatal("Describe succeeded after plugin crash")
	}
	if err := process.Close(); err == nil {
		t.Fatal("Close succeeded after plugin crash")
	}
	assertProcessGone(t, process)
}

func TestProcessTimeout(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
	defer cancel()

	process, err := Start(ctx, executable, "-test.run=^TestPluginTimeoutHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := process.Describe(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Describe error = %v, want deadline exceeded", err)
	}
	_ = process.Close()
	assertProcessGone(t, process)
}

func TestProcessOversizedDescribe(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	process, err := Start(ctx, executable, "-test.run=^TestPluginOversizedHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := process.Describe(ctx); !errors.Is(err, arpc.ErrMessageTooLarge) {
		t.Fatalf("Describe error = %v, want ErrMessageTooLarge", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	assertProcessGone(t, process)
}

func TestPluginProcessHelper(t *testing.T) {
	fdText, ok := os.LookupEnv(SocketFDEnv)
	if !ok {
		return
	}
	fd, err := strconv.Atoi(fdText)
	if err != nil {
		t.Fatalf("parse socket fd: %v", err)
	}

	file := os.NewFile(uintptr(fd), "pbs-plus-plugin")
	if file == nil {
		t.Fatal("open inherited socket")
	}
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		t.Fatalf("net.FileConn: %v", err)
	}

	pipe, err := arpc.NewServerPipe(t.Context(), conn)
	if err != nil {
		t.Fatalf("NewServerPipe: %v", err)
	}
	defer pipe.Close()

	router := arpc.NewRouter()
	router.Handle(MethodDescribe, func(request *arpc.Request) (arpc.Response, error) {
		var describe DescribeRequest
		if err := UnmarshalProtocol(request.Payload, &describe); err != nil {
			return arpc.Response{}, fmt.Errorf("decode describe request: %w", err)
		}
		if describe.ProtocolVersion != CurrentProtocolVersion {
			return arpc.Response{}, fmt.Errorf("unsupported host protocol %d", describe.ProtocolVersion)
		}
		data, err := MarshalProtocol(Descriptor{
			ProtocolVersion: CurrentProtocolVersion,
			PluginID:        "org.pbs-plus.test",
			Version:         "1.0.0",
			TargetTypes:     []string{"test"},
		})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode descriptor: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	pipe.SetRouter(router)
	_ = pipe.Serve()
}

func TestPluginCrashHelper(t *testing.T) {
	if _, ok := os.LookupEnv(SocketFDEnv); !ok {
		return
	}
	os.Exit(23)
}

func TestPluginTimeoutHelper(t *testing.T) {
	fdText, ok := os.LookupEnv(SocketFDEnv)
	if !ok {
		return
	}
	fd, err := strconv.Atoi(fdText)
	if err != nil {
		t.Fatalf("parse socket fd: %v", err)
	}

	file := os.NewFile(uintptr(fd), "pbs-plus-plugin")
	if file == nil {
		t.Fatal("open inherited socket")
	}
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		t.Fatalf("net.FileConn: %v", err)
	}

	pipe, err := arpc.NewServerPipe(t.Context(), conn)
	if err != nil {
		t.Fatalf("NewServerPipe: %v", err)
	}
	defer pipe.Close()

	router := arpc.NewRouter()
	router.Handle(MethodDescribe, func(*arpc.Request) (arpc.Response, error) {
		select {}
	})
	pipe.SetRouter(router)
	go func() { _ = pipe.Serve() }()
	select {}
}

func TestPluginOversizedHelper(t *testing.T) {
	fdText, ok := os.LookupEnv(SocketFDEnv)
	if !ok {
		return
	}
	fd, err := strconv.Atoi(fdText)
	if err != nil {
		t.Fatalf("parse socket fd: %v", err)
	}

	file := os.NewFile(uintptr(fd), "pbs-plus-plugin")
	if file == nil {
		t.Fatal("open inherited socket")
	}
	conn, err := net.FileConn(file)
	_ = file.Close()
	if err != nil {
		t.Fatalf("net.FileConn: %v", err)
	}

	pipe, err := arpc.NewServerPipe(t.Context(), conn)
	if err != nil {
		t.Fatalf("NewServerPipe: %v", err)
	}
	defer pipe.Close()

	router := arpc.NewRouter()
	router.Handle(MethodDescribe, func(*arpc.Request) (arpc.Response, error) {
		data, err := cbor.Marshal(Descriptor{
			ProtocolVersion: CurrentProtocolVersion,
			PluginID:        strings.Repeat("a", int(arpc.DefaultLocalMessageLimit)),
			Version:         "1.0.0",
			TargetTypes:     []string{"test"},
		})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode descriptor: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	pipe.SetRouter(router)
	_ = pipe.Serve()
}

func assertProcessGone(t *testing.T, process *Process) {
	t.Helper()
	if err := syscall.Kill(process.command.Process.Pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process still exists: %v", err)
	}
}
