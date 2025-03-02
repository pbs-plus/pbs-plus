package arpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/xtaci/smux"
)

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload []byte) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
}

func (s *Session) CallWithTimeout(timeout time.Duration, method string, payload []byte) (*Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallContext(ctx, method, payload)
}

// CallContext performs an RPC call over a new stream.
// It applies any context deadlines to the smux stream.
func (s *Session) CallContext(ctx context.Context, method string, payload []byte) (*Response, error) {

	// Use the atomic pointer to avoid holding a lock while reading.
	curSession := s.muxSess.Load().(*smux.Session)

	// Open a new stream.
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	// Propagate context deadlines to the stream.
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetWriteDeadline(deadline)
		stream.SetReadDeadline(deadline)
	}

	// Build and send the RPC request.
	req := Request{
		Method:  method,
		Payload: payload,
	}

	reqBytes, err := marshalWithPool(&req)
	if err != nil {
		return nil, err
	}
	defer reqBytes.Release()
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return nil, err
	}

	respBytes, err := readMsgpMsgPooled(stream)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}
	defer respBytes.Release()

	if len(respBytes.Data) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	var resp Response
	if _, err := resp.UnmarshalMsg(respBytes.Data); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CallMsg performs an RPC call and unmarshals its Data into v on success,
// or decodes the error from Data if status != http.StatusOK.
func (s *Session) CallMsg(ctx context.Context, method string, payload []byte) ([]byte, error) {
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		return nil, err
	}
	if resp.Status != http.StatusOK {
		if resp.Data != nil {
			var serErr SerializableError
			if _, err := serErr.UnmarshalMsg(resp.Data); err != nil {
				return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
			}
			return nil, UnwrapError(&serErr)
		}
		return nil, fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	if resp.Data == nil {
		return nil, nil
	}
	return resp.Data, nil
}

func (s *Session) CallMsgWithTimeout(timeout time.Duration, method string, payload []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallMsg(ctx, method, payload)
}

// CallMsgWithBuffer performs an RPC call for file I/O-style operations in which the server
// first sends metadata about a binary transfer and then writes the payload directly.
func (s *Session) CallMsgWithBuffer(ctx context.Context, method string, payload []byte, buffer []byte) (int, error) {
	curSession := s.muxSess.Load().(*smux.Session)

	// Single stream opening attempt with potential reconnect
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	// Build the request with direct buffer header
	req := Request{
		Method:  method,
		Payload: payload,
	}

	reqBytes, err := marshalWithPool(&req)
	if err != nil {
		return 0, err
	}
	defer reqBytes.Release()

	// Write request
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return 0, err
	}

	// Read metadata response
	metaBytes, err := readMsgpMsgPooled(stream)
	if err != nil {
		return 0, err
	}
	defer metaBytes.Release()

	// Handle non-OK status quickly
	var resp Response
	if _, err := resp.UnmarshalMsg(metaBytes.Data); err != nil {
		return 0, err
	}
	if resp.Status != 213 {
		var serErr SerializableError
		if _, err := serErr.UnmarshalMsg(metaBytes.Data); err == nil {
			return 0, UnwrapError(&serErr)
		}
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	// Create a buffered reader to improve efficiency
	bufReader := bufio.NewReaderSize(stream, 8192)

	// Read the length first
	var length uint32
	if err := binary.Read(bufReader, binary.LittleEndian, &length); err != nil {
		return 0, fmt.Errorf("failed to read length: %w", err)
	}

	// Now we know how much to read
	bytesToRead := min(int(length), len(buffer))
	bytesRead := 0

	for bytesRead < bytesToRead {
		n, err := bufReader.Read(buffer[bytesRead:bytesToRead])
		bytesRead += n

		if err != nil {
			if err == io.EOF && bytesRead == bytesToRead {
				return bytesRead, nil
			}
			return bytesRead, err
		}
	}

	return bytesRead, nil
}
