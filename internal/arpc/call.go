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
	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
	syslog.L.Debug().WithMessage("ARPC CallContext: begin").WithField("method", method).Write()

	stream, err := s.openStream(ctx)
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallContext: openStream failed").WithField("method", method).Write()
		return Response{}, err
	}
	defer stream.Close()

	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			syslog.L.Error(err).WithMessage("ARPC CallContext: payload encode failed").WithField("method", method).Write()
			return Response{}, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}
	reqBytes, err := req.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallContext: request encode failed").WithField("method", method).Write()
		return Response{}, fmt.Errorf("failed to encode request: %w", err)
	}

	if _, err := stream.Write(reqBytes); err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallContext: write request failed").WithField("method", method).Write()
		return Response{}, fmt.Errorf("failed to write request: %w", err)
	}

	prefix := headerPool.Get().([]byte)
	defer headerPool.Put(prefix)

	if _, err := io.ReadFull(stream, prefix); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			syslog.L.Warn().WithMessage("ARPC CallContext: read length prefix timeout").WithField("method", method).Write()
			return Response{}, context.DeadlineExceeded
		}
		if ctx.Err() != nil {
			syslog.L.Warn().WithMessage("ARPC CallContext: read full response timeout").WithField("method", method).Write()
			return Response{}, context.DeadlineExceeded
		}
		syslog.L.Error(err).WithMessage("ARPC CallContext: read length prefix failed").WithField("method", method).Write()
		return Response{}, fmt.Errorf("failed to read length prefix: %w", err)
	}
	totalLength := binary.LittleEndian.Uint32(prefix)
	if totalLength < 4 {
		err := fmt.Errorf("invalid total length %d", totalLength)
		syslog.L.Error(err).WithMessage("ARPC CallContext: invalid total length").WithField("method", method).Write()
		return Response{}, err
	}

	buf := make([]byte, totalLength)
	copy(buf, prefix)
	if _, err := io.ReadFull(stream, buf[4:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			syslog.L.Warn().WithMessage("ARPC CallContext: read full response timeout").WithField("method", method).Write()
			return Response{}, context.DeadlineExceeded
		}
		if ctx.Err() != nil {
			syslog.L.Warn().WithMessage("ARPC CallContext: read full response timeout").WithField("method", method).Write()
			return Response{}, context.DeadlineExceeded
		}
		syslog.L.Error(err).WithMessage("ARPC CallContext: read full response failed").WithField("method", method).Write()
		return Response{}, fmt.Errorf("failed to read full response: %w", err)
	}

	var resp Response
	if err := resp.Decode(buf); err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallContext: decode response failed").WithField("method", method).Write()
		return Response{}, fmt.Errorf("failed to decode response: %w", err)
	}

	syslog.L.Debug().WithMessage("ARPC CallContext: success").WithField("method", method).WithField("status", resp.Status).Write()
	return resp, nil
}

func (s *Session) CallMsg(ctx context.Context, method string, payload arpcdata.Encodable) ([]byte, error) {
	syslog.L.Debug().WithMessage("ARPC CallMsg: begin").WithField("method", method).Write()
	resp, err := s.CallContext(ctx, method, payload)
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallMsg: CallContext failed").WithField("method", method).Write()
		return nil, err
	}

	if resp.Status != http.StatusOK {
		if resp.Data != nil {
			var serErr SerializableError
			if err := serErr.Decode(resp.Data); err != nil {
				err2 := fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
				syslog.L.Error(err2).WithMessage("ARPC CallMsg: non-OK status, decode failed").WithField("method", method).WithField("status", resp.Status).Write()
				return nil, err2
			}
			err2 := UnwrapError(serErr)
			syslog.L.Error(err2).WithMessage("ARPC CallMsg: non-OK status").WithField("method", method).WithField("status", resp.Status).Write()
			return nil, err2
		}
		err2 := fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
		syslog.L.Error(err2).WithMessage("ARPC CallMsg: non-OK status, no data").WithField("method", method).WithField("status", resp.Status).Write()
		return nil, err2
	}

	if resp.Data == nil {
		syslog.L.Debug().WithMessage("ARPC CallMsg: success, no data").WithField("method", method).Write()
		return nil, nil
	}
	syslog.L.Debug().WithMessage("ARPC CallMsg: success").WithField("method", method).WithField("bytes", len(resp.Data)).Write()
	return resp.Data, nil
}

func (s *Session) CallMsgWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return s.CallMsg(ctx, method, payload)
}

func (s *Session) CallBinary(ctx context.Context, method string, payload arpcdata.Encodable, dst []byte) (int, error) {
	syslog.L.Debug().WithMessage("ARPC CallBinary: begin").WithField("method", method).Write()

	stream, err := s.openStream(ctx)
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallBinary: openStream failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			syslog.L.Error(err).WithMessage("ARPC CallBinary: payload encode failed").WithField("method", method).Write()
			return 0, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}

	reqBytes, err := req.Encode()
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallBinary: request encode failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to encode request: %w", err)
	}

	if _, err := stream.Write(reqBytes); err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallBinary: write request failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to write request: %w", err)
	}

	headerPrefix := headerPool.Get().([]byte)
	defer headerPool.Put(headerPrefix)

	if _, err := io.ReadFull(stream, headerPrefix); err != nil {
		if ctx.Err() != nil {
			syslog.L.Warn().WithMessage("ARPC CallBinary: read full response timeout").WithField("method", method).Write()
			return 0, context.DeadlineExceeded
		}
		syslog.L.Error(err).WithMessage("ARPC CallBinary: read header length prefix failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to read header length prefix: %w", err)
	}
	headerTotalLength := binary.LittleEndian.Uint32(headerPrefix)
	if headerTotalLength < 4 {
		err := fmt.Errorf("invalid header length %d", headerTotalLength)
		syslog.L.Error(err).WithMessage("ARPC CallBinary: invalid header length").WithField("method", method).Write()
		return 0, err
	}

	headerBuf := make([]byte, headerTotalLength)

	copy(headerBuf, headerPrefix)
	if _, err := io.ReadFull(stream, headerBuf[4:]); err != nil {
		if ctx.Err() != nil {
			syslog.L.Warn().WithMessage("ARPC CallBinary: read full response timeout").WithField("method", method).Write()
			return 0, context.DeadlineExceeded
		}
		syslog.L.Error(err).WithMessage("ARPC CallBinary: read full header failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to read full header: %w", err)
	}

	var resp Response
	if err := resp.Decode(headerBuf); err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallBinary: decode response failed").WithField("method", method).Write()
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Status != 213 {
		var serErr SerializableError
		if err := serErr.Decode(resp.Data); err == nil {
			uerr := UnwrapError(serErr)
			syslog.L.Error(uerr).WithMessage("ARPC CallBinary: server returned error").WithField("method", method).WithField("status", resp.Status).Write()
			return 0, uerr
		}
		err := fmt.Errorf("RPC error: status %d", resp.Status)
		syslog.L.Error(err).WithMessage("ARPC CallBinary: non-streaming status").WithField("method", method).Write()
		return 0, err
	}

	n, err := binarystream.ReceiveDataInto(stream, dst)
	if err != nil {
		syslog.L.Error(err).WithMessage("ARPC CallBinary: ReceiveDataInto failed").WithField("method", method).Write()
		return n, err
	}

	syslog.L.Debug().WithMessage("ARPC CallBinary: success").WithField("method", method).WithField("bytes", n).Write()
	return n, nil
}
