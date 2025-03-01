package arpc

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/xtaci/smux"
)

// Call initiates a request/response conversation on a new stream.
func (s *Session) Call(method string, payload []byte) (*Response, error) {
	return s.CallContext(context.Background(), method, payload)
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

	respBytes, err := readMsgpMsg(stream)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			return nil, context.DeadlineExceeded
		}
		return nil, err
	}
	if len(respBytes) == 0 {
		return nil, fmt.Errorf("empty response")
	}

	var resp Response
	if _, err := resp.UnmarshalMsg(respBytes); err != nil {
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

// CallMsgWithBuffer performs an RPC call for file I/O-style operations in which the server
// first sends metadata about a binary transfer and then writes the payload directly.
func (s *Session) CallMsgWithBuffer(ctx context.Context, method string, payload []byte, buffer []byte) (int, bool, error) {
	curSession := s.muxSess.Load().(*smux.Session)

	// Single stream opening attempt with potential reconnect
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return 0, false, err
	}
	defer stream.Close()

	// Set deadlines if provided in context
	if deadline, ok := ctx.Deadline(); ok {
		// Apply both deadlines with a single syscall where possible
		_ = stream.SetDeadline(deadline)
	}

	// Build the request with direct buffer header
	req := Request{
		Method:  method,
		Payload: payload,
		Headers: map[string]string{"X-Direct-Buffer": "true"},
	}

	reqBytes, err := marshalWithPool(&req)
	if err != nil {
		return 0, false, err
	}
	defer reqBytes.Release()

	// Write request
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return 0, false, err
	}

	// Read metadata response
	metaBytes, err := readMsgpMsg(stream)
	if err != nil {
		return 0, false, err
	}

	// Handle non-OK status quickly
	var resp Response
	if _, err := resp.UnmarshalMsg(metaBytes); err != nil {
		return 0, false, err
	}
	if resp.Status != 213 {
		var serErr SerializableError
		if _, err := serErr.UnmarshalMsg(metaBytes); err == nil {
			return 0, false, UnwrapError(&serErr)
		}
		return 0, false, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	// Parse buffer metadata
	var meta BufferMetadata
	if _, err := meta.UnmarshalMsg(resp.Data); err != nil {
		return 0, false, err
	}

	// Early return for empty content
	if meta.BytesAvailable <= 0 {
		return 0, meta.EOF, nil
	}

	// Calculate how much we can read into the provided buffer
	bytesToRead := min(meta.BytesAvailable, len(buffer))

	// Read directly into the provided buffer
	bytesRead := 0
	remaining := bytesToRead

	// Create a buffered reader to improve efficiency
	bufReader := bufio.NewReaderSize(stream, 8192)

	for bytesRead < bytesToRead {
		n, err := bufReader.Read(buffer[bytesRead:bytesToRead])
		bytesRead += n

		if err == io.EOF {
			// End of stream reached
			return bytesRead, meta.EOF, nil
		} else if err != nil {
			if ctx.Err() != nil {
				return bytesRead, false, ctx.Err() // Context timeout/cancellation takes precedence
			}
			return bytesRead, false, err
		}

		// If we got some data but not all, that's fine
		if n > 0 {
			remaining -= n
			if remaining == 0 {
				break
			}
		}
	}

	return bytesRead, meta.EOF, nil
}
