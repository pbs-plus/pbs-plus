package arpc

import (
	"context"
	"fmt"
	"net/http"

	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/xtaci/smux"
)

type Request struct {
	Context context.Context     `cbor:"-"`
	Method  string              `cbor:"method"`
	Payload []byte              `cbor:"payload"`
	Headers map[string][]string `cbor:"headers,omitempty"`
}

type Response struct {
	Status    int                `cbor:"status"`
	Message   string             `cbor:"message"`
	Data      []byte             `cbor:"data"`
	RawStream func(*smux.Stream) `cbor:"-"`
}

type SerializableError struct {
	ErrorType     string `cbor:"error_type"`
	Message       string `cbor:"message"`
	Op            string `cbor:"op"`
	Path          string `cbor:"path"`
	OriginalError error  `cbor:"-"`
}

type RawStreamHandler func(*smux.Stream) error

func (s *StreamPipe) call(ctx context.Context, method string, payload any) (*smux.Stream, *Response, error) {
	var stream *smux.Stream
	var err error

	stream, err = s.OpenStream()
	if err != nil {
		return nil, nil, err
	}

	enc := s.cborEnc.NewEncoder(stream)
	dec := s.cborDec.NewDecoder(stream)

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		default:
			payloadBytes, err = s.cborEnc.Marshal(p)
			if err != nil {
				return stream, nil, fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	headers := s.headers

	req := Request{Method: method, Payload: payloadBytes, Headers: headers}
	if err := enc.Encode(req); err != nil {
		return stream, nil, fmt.Errorf("write request: %w", err)
	}

	var resp Response
	if err := dec.Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return stream, nil, ctx.Err()
		}
		return stream, nil, fmt.Errorf("decode response: %w", err)
	}

	return stream, &resp, nil
}

func (s *StreamPipe) Call(ctx context.Context, method string, payload any, out any) error {
	stream, resp, err := s.call(ctx, method, payload)
	if err != nil {
		return err
	}
	defer stream.Close()

	if resp.Status == 213 {
		handler, ok := out.(RawStreamHandler)
		if !ok || handler == nil {
			return fmt.Errorf("invalid out handler while in raw stream mode")
		}

		readySignal := []byte{0xFF}
		if _, err := stream.Write(readySignal); err != nil {
			return fmt.Errorf("write ready signal: %w", err)
		}

		ackByte := make([]byte, 1)
		if _, err := stream.Read(ackByte); err != nil {
			return fmt.Errorf("read ack signal: %w", err)
		}
		if ackByte[0] != 0xAA {
			return fmt.Errorf("invalid ack signal: expected 0xAA, got 0x%02X", ackByte[0])
		}

		err = handler(stream)
		if err != nil {
			return err
		}
		return nil
	}

	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := s.cborDec.Unmarshal(resp.Data, &serErr); err == nil {
				return UnwrapError(serErr)
			}
		}
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	if out == nil || len(resp.Data) == 0 {
		return nil
	}
	switch dst := out.(type) {
	case *[]byte:
		*dst = append((*dst)[:0], resp.Data...)
		return nil
	default:
		return s.cborDec.Unmarshal(resp.Data, out)
	}
}

func (s *StreamPipe) CallData(ctx context.Context, method string, payload any) ([]byte, error) {
	var out []byte
	if err := s.Call(ctx, method, payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *StreamPipe) CallMessage(ctx context.Context, method string, payload any) (string, error) {
	stream, resp, err := s.call(ctx, method, payload)
	if err != nil {
		return "", err
	}
	defer stream.Close()

	if resp.Status == 213 {
		return "", fmt.Errorf("RPC error: raw stream not supported by CallMessage (status %d)", resp.Status)
	}

	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := s.cborDec.Unmarshal(resp.Data, &serErr); err == nil {
				return "", UnwrapError(serErr)
			}
		}
		return "", fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	return resp.Message, nil
}

func (s *StreamPipe) CallBinary(ctx context.Context, method string, payload any, dst []byte) (int, error) {
	stream, resp, err := s.call(ctx, method, payload)
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	if resp.Status != 213 {
		var serErr SerializableError
		if err := s.cborDec.Unmarshal(resp.Data, &serErr); err == nil {
			return 0, UnwrapError(serErr)
		}
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	readySignal := []byte{0xFF}
	if _, err := stream.Write(readySignal); err != nil {
		return 0, fmt.Errorf("write ready signal: %w", err)
	}

	ackByte := make([]byte, 1)
	if _, err := stream.Read(ackByte); err != nil {
		return 0, fmt.Errorf("read ack signal: %w", err)
	}
	if ackByte[0] != 0xAA {
		return 0, fmt.Errorf("invalid ack signal: expected 0xAA, got 0x%02X", ackByte[0])
	}

	return binarystream.ReceiveDataInto(stream, dst)
}
