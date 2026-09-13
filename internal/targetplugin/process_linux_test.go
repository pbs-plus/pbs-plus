package targetplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
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

func TestProcessInvoke(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	process, err := Start(t.Context(), executable, "-test.run=^TestPluginInvokeHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	request := TargetProbeRequest{
		Operation: validTestOperation(),
		Target:    TargetInput{Config: Values{"path": NewStringScalar("/data")}},
	}
	var response TargetProbeResponse
	if err := process.Invoke(t.Context(), MethodTargetProbe, request, &response); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if !response.Available {
		t.Fatal("target is unavailable")
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessInvokeRejectsInvalidRequest(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	process, err := Start(t.Context(), executable, "-test.run=^TestPluginInvokeHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	var response TargetProbeResponse
	err = process.Invoke(t.Context(), MethodTargetProbe, TargetProbeRequest{}, &response)
	if err == nil || !strings.Contains(err.Error(), "validate plugin protocol request") {
		t.Fatalf("Invoke error = %v, want request validation error", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessInvokeRejectsInvalidResponse(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	process, err := Start(t.Context(), executable, "-test.run=^TestPluginInvokeHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	operation := validTestOperation()
	operation.TargetType = ""
	operation.SchemaVersion = 0
	var response PluginHealthResponse
	err = process.Invoke(t.Context(), MethodPluginHealth, PluginHealthRequest{Operation: operation}, &response)
	if err == nil || !strings.Contains(err.Error(), "validate plugin protocol response") {
		t.Fatalf("Invoke error = %v, want response validation error", err)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessInvokeRawStream(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	process, err := Start(t.Context(), executable, "-test.run=^TestPluginInvokeHelper$")
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	var data []byte
	handler := arpc.RawStreamHandler(func(stream arpc.ARPCStream) error {
		var err error
		data, err = io.ReadAll(stream)
		return err
	})
	var metadata BackupOpenResponse
	response := &RawStreamResponse{Metadata: &metadata, Handle: handler}
	request := BackupOpenRequest{Operation: jobTestOperation(), Job: validJobInput()}
	if err := process.Invoke(t.Context(), MethodBackupOpen, request, response); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if metadata.Kind != SourceRawStream {
		t.Fatalf("metadata kind = %q, want %q", metadata.Kind, SourceRawStream)
	}
	if string(data) != "bulk-data" {
		t.Fatalf("raw data = %q, want bulk-data", data)
	}
	if err := process.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestProcessInvokeRejectsNonOperationMethod(t *testing.T) {
	err := (*Process)(nil).Invoke(t.Context(), MethodDescribe, PluginHealthRequest{}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported plugin invocation method") {
		t.Fatalf("Invoke error = %v, want unsupported method", err)
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
			TargetSchema:    FormSchema{Version: 1},
			BackupSchema:    FormSchema{Version: 1},
			RestoreSchema:   FormSchema{Version: 1},
		})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode descriptor: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	router.Handle(MethodPluginHealth, func(request *arpc.Request) (arpc.Response, error) {
		var health PluginHealthRequest
		if err := UnmarshalProtocol(request.Payload, &health); err != nil {
			return arpc.Response{}, fmt.Errorf("decode health request: %w", err)
		}
		response := PluginHealthResponse{Healthy: true}
		if os.Getenv("PBS_PLUS_TEST_PLUGIN_UNHEALTHY") == "1" {
			response = PluginHealthResponse{Message: "test failure"}
		}
		data, err := MarshalProtocol(response)
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode health response: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	pipe.SetRouter(router)
	_ = pipe.Serve()
}

func TestPluginInvokeHelper(t *testing.T) {
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
	router.Handle(MethodTargetProbe, func(request *arpc.Request) (arpc.Response, error) {
		var probe TargetProbeRequest
		if err := UnmarshalProtocol(request.Payload, &probe); err != nil {
			return arpc.Response{}, fmt.Errorf("decode probe request: %w", err)
		}
		if err := probe.Validate(); err != nil {
			return arpc.Response{}, fmt.Errorf("validate probe operation: %w", err)
		}
		data, err := MarshalProtocol(TargetProbeResponse{Available: true})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode probe response: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	router.Handle(MethodPluginHealth, func(_ *arpc.Request) (arpc.Response, error) {
		data, err := MarshalProtocol(PluginHealthResponse{})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode health response: %w", err)
		}
		return arpc.Response{Status: http.StatusOK, Data: data}, nil
	})
	router.Handle(MethodBackupOpen, func(request *arpc.Request) (arpc.Response, error) {
		var open BackupOpenRequest
		if err := UnmarshalProtocol(request.Payload, &open); err != nil {
			return arpc.Response{}, fmt.Errorf("decode backup request: %w", err)
		}
		if err := open.Validate(); err != nil {
			return arpc.Response{}, fmt.Errorf("validate backup operation: %w", err)
		}
		data, err := MarshalProtocol(BackupOpenResponse{
			Kind:         SourceRawStream,
			Archive:      Archive{Type: "pxar", FormatVersion: 1},
			CleanupToken: []byte("lease"),
		})
		if err != nil {
			return arpc.Response{}, fmt.Errorf("encode backup response: %w", err)
		}
		return arpc.Response{Status: arpc.StatusRawStream, Data: data, RawStream: func(stream arpc.ARPCStream) {
			_, _ = stream.Write([]byte("bulk-data"))
		}}, nil
	})
	pipe.SetRouter(router)
	_ = pipe.Serve()
}

func validTestOperation() Operation {
	return Operation{
		ProtocolVersion:   CurrentProtocolVersion,
		ID:                "op-1",
		IdempotencyKey:    "retry-1",
		DeadlineUnixMilli: time.Now().Add(time.Minute).UnixMilli(),
		PluginVersion:     "1.0.0",
		TargetType:        "test",
		SchemaVersion:     1,
	}
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
			TargetSchema:    FormSchema{Version: 1},
			BackupSchema:    FormSchema{Version: 1},
			RestoreSchema:   FormSchema{Version: 1},
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
