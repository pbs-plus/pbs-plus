package arpc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/quic-go/quic-go"
)

var (
	cborEncMode cbor.EncMode
	cborDecMode cbor.DecMode
)

func init() {
	em, err := cbor.CTAP2EncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	dm, err := cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(err)
	}
	cborEncMode = em
	cborDecMode = dm
}

type Encodable interface {
	Encode() ([]byte, error)
	Decode([]byte) error
}

type Request struct {
	Method  string              `cbor:"method"`
	Payload []byte              `cbor:"payload"`
	Headers map[string][]string `cbor:"headers,omitempty"`
}

func (r *Request) Encode() ([]byte, error) {
	return cborEncMode.Marshal(r)
}

func (r *Request) Decode(b []byte) error {
	return cborDecMode.Unmarshal(b, r)
}

type Response struct {
	Status    int                `cbor:"status"`
	Message   string             `cbor:"message"`
	Data      []byte             `cbor:"data"`
	RawStream func(*quic.Stream) `cbor:"-"`
}

func (r *Response) Encode() ([]byte, error) {
	return cborEncMode.Marshal(r)
}

func (r *Response) Decode(b []byte) error {
	return cborDecMode.Unmarshal(b, r)
}

type SerializableError struct {
	ErrorType     string `cbor:"error_type"`
	Message       string `cbor:"message"`
	Op            string `cbor:"op"`
	Path          string `cbor:"path"`
	OriginalError error  `cbor:"-"`
}

func (e *SerializableError) Encode() ([]byte, error) {
	return cborEncMode.Marshal(e)
}

func (e *SerializableError) Decode(b []byte) error {
	return cborDecMode.Unmarshal(b, e)
}

type RawStreamHandler func(*quic.Stream) error

func (s *StreamPipe) Call(ctx context.Context, method string, payload any, out any) error {
	stream, err := s.openStream(ctx)
	if err != nil {
		return err
	}

	defer stream.Close()
	defer stream.CancelRead(0)

	if deadline, ok := ctx.Deadline(); ok {
		stream.SetDeadline(deadline)
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		case Encodable:
			payloadBytes, err = p.Encode()
			if err != nil {
				stream.CancelWrite(0)
				return fmt.Errorf("encode payload: %w", err)
			}
		default:
			payloadBytes, err = cborEncMode.Marshal(p)
			if err != nil {
				stream.CancelWrite(0)
				return fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	req := Request{Method: method, Payload: payloadBytes, Headers: headerCloneMap(s.headers)}
	reqBytes, err := req.Encode()
	if err != nil {
		stream.CancelWrite(0)
		return fmt.Errorf("encode request: %w", err)
	}
	if _, err := stream.Write(reqBytes); err != nil {
		stream.CancelWrite(0)
		return fmt.Errorf("write request: %w", err)
	}

	// Read response
	dec := cbor.NewDecoder(stream)
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		stream.CancelWrite(0)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("decode response: %w", err)
	}

	// Raw stream path
	if resp.Status == 213 {
		handler, ok := out.(RawStreamHandler)
		if !ok || handler == nil {
			stream.CancelWrite(0)
			return fmt.Errorf("invalid out handler while in raw stream mode")
		}
		if _, err := stream.Write([]byte{0x01}); err != nil {
			stream.CancelWrite(0)
			return fmt.Errorf("raw ready signal write failed: %w", err)
		}
		err = handler(stream)
		if err != nil {
			stream.CancelWrite(0)
			return err
		}
		return nil
	}

	// Normal error path
	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := serErr.Decode(resp.Data); err == nil {
				stream.CancelWrite(0)
				return UnwrapError(serErr)
			}
		}
		stream.CancelWrite(0)
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	// Normal success
	if out == nil || len(resp.Data) == 0 {
		return nil
	}
	switch dst := out.(type) {
	case *[]byte:
		*dst = append((*dst)[:0], resp.Data...)
		return nil
	default:
		return cborDecMode.Unmarshal(resp.Data, out)
	}
}

func (s *StreamPipe) CallWithTimeout(timeout time.Duration, method string, payload any, out any) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.Call(ctx, method, payload, out)
}

func (s *StreamPipe) CallData(ctx context.Context, method string, payload any) ([]byte, error) {
	var out []byte
	if err := s.Call(ctx, method, payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *StreamPipe) CallDataWithTimeout(timeout time.Duration, method string, payload any) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.CallData(ctx, method, payload)
}

func (s *StreamPipe) CallMessage(ctx context.Context, method string, payload any) (string, error) {
	stream, err := s.openStream(ctx)
	if err != nil {
		return "", err
	}

	defer stream.Close()
	defer stream.CancelRead(0)

	if deadline, ok := ctx.Deadline(); ok {
		stream.SetDeadline(deadline)
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		case Encodable:
			payloadBytes, err = p.Encode()
			if err != nil {
				stream.CancelWrite(0)
				return "", fmt.Errorf("encode payload: %w", err)
			}
		default:
			payloadBytes, err = cborEncMode.Marshal(p)
			if err != nil {
				stream.CancelWrite(0)
				return "", fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	req := Request{Method: method, Payload: payloadBytes}
	reqBytes, err := req.Encode()
	if err != nil {
		stream.CancelWrite(0)
		return "", fmt.Errorf("encode request: %w", err)
	}
	if _, err := stream.Write(reqBytes); err != nil {
		stream.CancelWrite(0)
		return "", fmt.Errorf("write request: %w", err)
	}

	dec := cbor.NewDecoder(stream)
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		stream.CancelWrite(0)
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("decode response: %w", err)
	}

	if resp.Status == 213 {
		stream.CancelWrite(0)
		return "", fmt.Errorf("RPC error: raw stream not supported by CallMessage (status %d)", resp.Status)
	}

	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := serErr.Decode(resp.Data); err == nil {
				stream.CancelWrite(0)
				return "", UnwrapError(serErr)
			}
		}
		stream.CancelWrite(0)
		return "", fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	return resp.Message, nil
}

func (s *StreamPipe) CallMessageWithTimeout(timeout time.Duration, method string, payload any) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return s.CallMessage(ctx, method, payload)
}
