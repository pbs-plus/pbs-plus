//go:build linux

package targetplugin

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

// MethodHandler answers one plugin method; the returned value is encoded as canonical CBOR.
type MethodHandler func(ctx context.Context, payload []byte) (any, error)

type hostPipeContextKey struct{}

// Serve answers plugin methods over the socket the host inherited to this process.
func Serve(ctx context.Context, descriptor Descriptor, handlers map[string]MethodHandler) error {
	if err := descriptor.Validate(); err != nil {
		return fmt.Errorf("plugin descriptor: %w", err)
	}
	for method := range handlers {
		if err := validateInvocationMethod(method); err != nil {
			return err
		}
	}
	pipe, err := inheritedPipe(ctx)
	if err != nil {
		return err
	}
	defer pipe.Close()

	router := arpc.NewRouter()
	router.Handle(MethodDescribe, func(request *arpc.Request) (arpc.Response, error) {
		var describe DescribeRequest
		if err := UnmarshalProtocol(request.Payload, &describe); err != nil {
			return arpc.Response{}, err
		}
		if describe.ProtocolVersion != CurrentProtocolVersion {
			return arpc.Response{}, fmt.Errorf("unsupported host protocol %d", describe.ProtocolVersion)
		}
		return protocolResponse(descriptor)
	})
	for method, handler := range handlers {
		router.Handle(method, func(request *arpc.Request) (arpc.Response, error) {
			handlerCtx := context.WithValue(request.Context, hostPipeContextKey{}, pipe)
			value, err := handler(handlerCtx, request.Payload)
			if err != nil {
				return arpc.Response{}, err
			}
			return protocolResponse(value)
		})
	}
	pipe.SetRouter(router)
	serveErr := pipe.Serve()
	if hostClosed(serveErr) {
		return nil
	}
	return serveErr
}

// hostClosed covers every teardown error a closed plugin socket produces, including the ECONNRESET a host close races into.
func hostClosed(err error) bool {
	return err == nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE)
}

// Request decodes and validates one plugin request payload.
func Request[T interface{ Validate() error }](payload []byte) (T, error) {
	var request T
	if err := UnmarshalProtocol(payload, &request); err != nil {
		return request, err
	}
	if err := request.Validate(); err != nil {
		return request, err
	}
	return request, nil
}

func CallHost(ctx context.Context, method string, request interface{ Validate() error }, response any) error {
	if ctx == nil {
		return errors.New("host call context is required")
	}
	pipe, ok := ctx.Value(hostPipeContextKey{}).(*arpc.StreamPipe)
	if !ok || pipe == nil {
		return errors.New("host calls are only available inside a plugin handler")
	}
	if err := validateHostInvocationMethod(method); err != nil {
		return err
	}
	if err := validateProtocolValue("host request", request); err != nil {
		return err
	}
	payload, err := MarshalProtocol(request)
	if err != nil {
		return fmt.Errorf("encode host request: %w", err)
	}
	var data []byte
	if err := pipe.Call(ctx, method, payload, &data); err != nil {
		return fmt.Errorf("invoke %s: %w", method, err)
	}
	if response == nil {
		return nil
	}
	if len(data) == 0 {
		return errors.New("host response is empty")
	}
	if err := UnmarshalProtocol(data, response); err != nil {
		return fmt.Errorf("decode host response: %w", err)
	}
	return validateProtocolValue("host response", response)
}

type hostEventWriter struct {
	ctx       context.Context
	operation Operation
	level     EventLevel
}

// NewHostEventWriter converts line-oriented tool output into authenticated host events.
func NewHostEventWriter(ctx context.Context, operation Operation, level EventLevel) io.Writer {
	return &hostEventWriter{ctx: ctx, operation: operation, level: level}
}

// NewJobEventLog opens an info-level host-event log and writes its section marker.
func NewJobEventLog(ctx context.Context, operation Operation, label string) (io.Writer, error) {
	writer := NewHostEventWriter(ctx, operation, EventInfo)
	if _, err := fmt.Fprintf(writer, "--- %s log starts here ---\n", label); err != nil {
		return nil, err
	}
	return writer, nil
}

// NewLeaseToken mints the random cleanup token a plugin returns for its lease.
func NewLeaseToken() ([]byte, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("create lease token: %w", err)
	}
	return token, nil
}

func (writer *hostEventWriter) Write(data []byte) (int, error) {
	for line := range strings.SplitSeq(strings.TrimSuffix(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		if err := CallHost(writer.ctx, MethodHostEvent, HostEvent{
			Operation: writer.operation,
			Level:     writer.level,
			Message:   line,
		}, nil); err != nil {
			return 0, err
		}
	}
	return len(data), nil
}

func validateHostInvocationMethod(method string) error {
	switch method {
	case MethodHostEvent, MethodHostScratch, MethodHostAgentBackupMount, MethodHostAgentRestore, MethodHostLeaseClose:
		return nil
	default:
		return fmt.Errorf("unsupported host invocation method %q", method)
	}
}

func inheritedPipe(ctx context.Context) (*arpc.StreamPipe, error) {
	descriptorText, ok := os.LookupEnv(SocketFDEnv)
	if !ok {
		return nil, fmt.Errorf("%s is not set; this executable is started by pbs-plus", SocketFDEnv)
	}
	fd, err := strconv.Atoi(descriptorText)
	if err != nil {
		return nil, fmt.Errorf("invalid %s value %q: %w", SocketFDEnv, descriptorText, err)
	}
	file := os.NewFile(uintptr(fd), "pbs-plus-plugin-socket")
	if file == nil {
		return nil, errors.New("inherited plugin socket is not usable")
	}
	defer file.Close()
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, fmt.Errorf("open inherited plugin socket: %w", err)
	}
	pipe, err := arpc.NewServerPipe(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("serve inherited plugin socket: %w", err)
	}
	return pipe, nil
}

func protocolResponse(value any) (arpc.Response, error) {
	data, err := MarshalProtocol(value)
	if err != nil {
		return arpc.Response{}, err
	}
	return arpc.Response{Status: http.StatusOK, Data: data}, nil
}
