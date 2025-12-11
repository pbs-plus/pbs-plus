package arpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/arpc/arpcdata"
	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
)

var headerPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 4)
	},
}

func (s *Session) Call(method string, payload arpcdata.Encodable) (Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

func (s *Session) CallWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) (Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallContext(ctx, method, payload)
}

func (s *Session) CallContext(ctx context.Context, method string, payload arpcdata.Encodable) (Response, error) {
	stream, err := s.openStream()
	if err != nil {
		return Response{}, err
	}
	defer func() { _ = stream.Close() }()

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			return Response{}, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}
	reqBytes, err := req.Encode()
	if err != nil {
		return Response{}, fmt.Errorf("failed to encode request: %w", err)
	}

	if _, err := stream.Write(reqBytes); err != nil {
		return Response{}, fmt.Errorf("failed to write request: %w", err)
	}

	prefix := headerPool.Get().([]byte)
	defer headerPool.Put(prefix)

	if _, err := io.ReadFull(stream, prefix); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			_ = stream.Close()
			return Response{}, context.DeadlineExceeded
		}
		_ = stream.Close()
		return Response{}, fmt.Errorf("failed to read length prefix: %w", err)
	}
	totalLength := binary.LittleEndian.Uint32(prefix)
	if totalLength < 4 {
		_ = stream.Close()
		return Response{}, fmt.Errorf("invalid total length %d", totalLength)
	}

	buf := make([]byte, totalLength)
	copy(buf, prefix)
	if _, err := io.ReadFull(stream, buf[4:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			_ = stream.Close()
			return Response{}, context.DeadlineExceeded
		}
		_ = stream.Close()
		return Response{}, fmt.Errorf("failed to read full response: %w", err)
	}

	var resp Response
	if err := resp.Decode(buf); err != nil {
		_ = stream.Close()
		return Response{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

func (s *Session) CallMsg(ctx context.Context, method string, payload arpcdata.Encodable) ([]byte, error) {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return nil, err
	}

	if resp.Status != http.StatusOK {
		if resp.Data != nil {
			var serErr SerializableError
			if err := serErr.Decode(resp.Data); err != nil {
				return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
			}
			return nil, UnwrapError(serErr)
		}
		return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	if resp.Data == nil {
		return nil, nil
	}
	return resp.Data, nil
}

func (s *Session) CallMsgWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallMsg(ctx, method, payload)
}

func (s *Session) CallBinary(ctx context.Context, method string, payload arpcdata.Encodable, dst []byte) (int, error) {
	stream, err := s.openStream()
	if err != nil {
		return 0, fmt.Errorf("failed to open stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			return 0, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}

	reqBytes, err := req.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode request: %w", err)
	}

	if _, err := stream.Write(reqBytes); err != nil {
		return 0, fmt.Errorf("failed to write request: %w", err)
	}

	headerPrefix := headerPool.Get().([]byte)
	defer headerPool.Put(headerPrefix)

	if _, err := io.ReadFull(stream, headerPrefix); err != nil {
		_ = stream.Close()
		return 0, fmt.Errorf("failed to read header length prefix: %w", err)
	}
	headerTotalLength := binary.LittleEndian.Uint32(headerPrefix)
	if headerTotalLength < 4 {
		_ = stream.Close()
		return 0, fmt.Errorf("invalid header length %d", headerTotalLength)
	}

	headerBuf := make([]byte, headerTotalLength)
	copy(headerBuf, headerPrefix)
	if _, err := io.ReadFull(stream, headerBuf[4:]); err != nil {
		_ = stream.Close()
		return 0, fmt.Errorf("failed to read full header: %w", err)
	}

	var resp Response
	if err := resp.Decode(headerBuf); err != nil {
		_ = stream.Close()
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Status != 213 {
		var serErr SerializableError
		if err := serErr.Decode(resp.Data); err == nil {
			_ = stream.Close()
			return 0, UnwrapError(serErr)
		}
		_ = stream.Close()
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	n, err := binarystream.ReceiveDataInto(stream, dst)
	if err != nil {
		_ = stream.Close()
		return n, err
	}
	return n, nil
}
