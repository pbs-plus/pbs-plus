package arpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
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
	log.Printf("Starting CallMsgWithBuffer for method: %s", method)

	curSession := s.muxSess.Load().(*smux.Session)
	stream, err := openStreamWithReconnect(s, curSession)
	if err != nil {
		return 0, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	if deadline, ok := ctx.Deadline(); ok {
		log.Print("Setting stream deadline")
		_ = stream.SetDeadline(deadline)
	}

	// Send request
	req := Request{
		Method:  method,
		Payload: payload,
	}

	reqBytes, err := marshalWithPool(&req)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal request: %w", err)
	}
	defer reqBytes.Release()

	log.Print("Writing request to stream")
	if err := writeMsgpMsg(stream, reqBytes.Data); err != nil {
		return 0, fmt.Errorf("failed to write request: %w", err)
	}

	log.Print("Reading response status")
	// Read response status
	respBytes, err := readMsgpMsgPooled(stream)
	if err != nil {
		return 0, fmt.Errorf("failed to read response: %w", err)
	}
	defer respBytes.Release()

	var resp Response
	if _, err := resp.UnmarshalMsg(respBytes.Data); err != nil {
		return 0, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	log.Printf("Got response status: %d", resp.Status)

	if resp.Status != 213 {
		var serErr SerializableError
		if _, err := serErr.UnmarshalMsg(respBytes.Data); err == nil {
			return 0, UnwrapError(&serErr)
		}
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	log.Print("Reading length prefix")
	// Read the length prefix
	var length uint32
	if err := binary.Read(stream, binary.LittleEndian, &length); err != nil {
		return 0, fmt.Errorf("failed to read length prefix: %w", err)
	}
	log.Printf("Got length prefix: %d", length)

	// Ensure we don't exceed buffer capacity
	bytesToRead := min(int(length), len(buffer))

	// Read the data
	totalRead := 0
	for totalRead < bytesToRead {
		n, err := stream.Read(buffer[totalRead:bytesToRead])
		if n > 0 {
			totalRead += n
			log.Printf("Read %d bytes (total: %d/%d)", n, totalRead, bytesToRead)
		}
		if err != nil {
			if err == io.EOF && totalRead == bytesToRead {
				log.Print("Reached EOF after reading all data")
				return totalRead, nil
			}
			return totalRead, fmt.Errorf("read error after %d/%d bytes: %w",
				totalRead, bytesToRead, err)
		}
	}

	log.Printf("Successfully read all %d bytes", totalRead)
	return totalRead, nil
}
