package arpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/arpc/arpcdata"
	binarystream "github.com/sonroyaalmerol/pbs-plus/internal/arpc/binary"
)

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload arpcdata.Encodable) (Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

func (s *Session) CallWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) (Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallContext(ctx, method, payload)
}

// CallContext performs an RPC call over a new stream.
// It applies any context deadlines to the smux stream.
func (s *Session) CallContext(ctx context.Context, method string, payload arpcdata.Encodable) (Response, error) {
	curSession := s.muxSess.Load()

	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return Response{}, err
	}
	defer stream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
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

	var prefix [4]byte
	if _, err := io.ReadFull(stream, prefix[:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return Response{}, context.DeadlineExceeded
		}
		return Response{}, fmt.Errorf("failed to read length prefix: %w", err)
	}

	totalLength := binary.LittleEndian.Uint32(prefix[:])
	if totalLength < 4 {
		return Response{}, fmt.Errorf("invalid total length %d", totalLength)
	}

	// Allocate a buffer to hold the entire response (the prefix is already read).
	buf := make([]byte, totalLength)
	// Copy the already-read 4-byte prefix into the buffer.
	copy(buf, prefix[:])
	// Read the remaining bytes of the response.
	if _, err := io.ReadFull(stream, buf[4:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return Response{}, context.DeadlineExceeded
		}
		return Response{}, fmt.Errorf("failed to read full response: %w", err)
	}

	var resp Response
	if err := resp.Decode(buf); err != nil {
		return Response{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

// CallMsg performs an RPC call and unmarshals its Data into v on success,
// or decodes the error from Data if status != http.StatusOK.
func (s *Session) CallMsg(ctx context.Context, method string, payload arpcdata.Encodable) ([]byte, error) {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return nil, err
	}

	// Handle error responses
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

	// Return the response data
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

// CallBinary performs an RPC call for file I/O-style operations in which the server
// first sends metadata about a binary transfer and then writes the payload directly.
func (s *Session) CallBinary(ctx context.Context, method string, payload arpcdata.Encodable, buffer []byte) (int, error) {
	curSession := s.muxSess.Load()
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return 0, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

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

	var prefix [4]byte
	if _, err := io.ReadFull(stream, prefix[:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return 0, context.DeadlineExceeded
		}
		return 0, fmt.Errorf("failed to read header length prefix: %w", err)
	}
	totalLength := binary.LittleEndian.Uint32(prefix[:])
	if totalLength < 4 {
		return 0, fmt.Errorf("invalid header length %d", totalLength)
	}

	var headerBytes []byte
	if totalLength == 4 {
		headerBytes = prefix[:]
	} else {
		headerBytes = make([]byte, totalLength)
		copy(headerBytes, prefix[:])
		if _, err := io.ReadFull(stream, headerBytes[4:]); err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return 0, context.DeadlineExceeded
			}
			return 0, fmt.Errorf("failed to read full header: %w", err)
		}
	}

	var resp Response
	if err := resp.Decode(headerBytes); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Status != 213 {
		var serErr SerializableError
		if err := serErr.Decode(resp.Data); err == nil {
			return 0, UnwrapError(serErr)
		}
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	return binarystream.ReceiveData(stream, buffer)
}
