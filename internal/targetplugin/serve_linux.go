//go:build linux

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

	"github.com/pbs-plus/pbs-plus/internal/arpc"
)

// MethodHandler answers one plugin method; the returned value is encoded as canonical CBOR.
type MethodHandler func(ctx context.Context, payload []byte) (any, error)

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
			value, err := handler(request.Context, request.Payload)
			if err != nil {
				return arpc.Response{}, err
			}
			return protocolResponse(value)
		})
	}
	pipe.SetRouter(router)
	serveErr := pipe.Serve()
	if serveErr == nil || errors.Is(serveErr, context.Canceled) || errors.Is(serveErr, io.EOF) || errors.Is(serveErr, io.ErrClosedPipe) {
		return nil
	}
	return serveErr
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
