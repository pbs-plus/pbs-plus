package arpc

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fxamacker/cbor/v2"
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

func (s *StreamPipe) Call(ctx context.Context, method string, payload any, out any) error {
	stream, err := s.OpenStream()
	if err != nil {
		return err
	}

	var cleanupOnce sync.Once
	cleanup := func() {
		stream.Close()
	}
	defer cleanupOnce.Do(cleanup)

	var streaming atomic.Bool
	streaming.Store(false)

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	if ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			if streaming.Load() {
				return
			}
			cleanupOnce.Do(cleanup)
		}()
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		default:
			payloadBytes, err = cbor.Marshal(p)
			if err != nil {
				return fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	s.RLock()
	headers := headerCloneMap(s.headers)
	s.RUnlock()

	req := Request{Method: method, Payload: payloadBytes, Headers: headers}
	reqBytes, err := cbor.Marshal(req)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if _, err := stream.Write(reqBytes); err != nil {
		return fmt.Errorf("write request: %w", err)
	}

	dec := cbor.NewDecoder(stream)
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("decode response: %w", err)
	}

	if resp.Status == 213 {
		handler, ok := out.(RawStreamHandler)
		if !ok || handler == nil {
			return fmt.Errorf("invalid out handler while in raw stream mode")
		}
		if _, err := stream.Write([]byte{0x01}); err != nil {
			return fmt.Errorf("raw ready signal write failed: %w", err)
		}

		_ = stream.SetDeadline(time.Time{})
		streaming.Store(true)
		defer streaming.Store(false)

		err = handler(stream)
		if err != nil {
			return err
		}
		return nil
	}

	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := cbor.Unmarshal(resp.Data, &serErr); err == nil {
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
		return cbor.Unmarshal(resp.Data, out)
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
	stream, err := s.OpenStream()
	if err != nil {
		return "", err
	}

	var cleanupOnce sync.Once
	cleanup := func() {
		stream.Close()
	}
	defer cleanupOnce.Do(cleanup)

	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}

	if ctx.Done() != nil {
		go func() {
			<-ctx.Done()
			cleanupOnce.Do(cleanup)
		}()
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		default:
			payloadBytes, err = cbor.Marshal(p)
			if err != nil {
				return "", fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	req := Request{Method: method, Payload: payloadBytes}
	reqBytes, err := cbor.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("encode request: %w", err)
	}
	if _, err := stream.Write(reqBytes); err != nil {
		return "", fmt.Errorf("write request: %w", err)
	}

	dec := cbor.NewDecoder(stream)
	var resp Response
	if err := dec.Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", fmt.Errorf("decode response: %w", err)
	}

	if resp.Status == 213 {
		return "", fmt.Errorf("RPC error: raw stream not supported by CallMessage (status %d)", resp.Status)
	}

	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := cbor.Unmarshal(resp.Data, &serErr); err == nil {
				return "", UnwrapError(serErr)
			}
		}
		return "", fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}

	return resp.Message, nil
}
