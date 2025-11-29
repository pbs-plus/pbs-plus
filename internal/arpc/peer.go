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

type Peer struct {
	addr      string
	transport Transport
	ctx       context.Context
	mu        sync.RWMutex
	closed    bool
}

// getConnection creates a new connection for a single request
func (p *Peer) getConnection(ctx context.Context) (net.Conn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("peer is closed")
	}
	p.mu.RUnlock()

	conn, err := p.transport.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer: %w", err)
	}
	return conn, nil
}

// Addr returns the peer's address
func (p *Peer) Addr() string {
	return p.addr
}

func (p *Peer) IsClosed() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.closed
}

func (p *Peer) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true
	return nil
}

// Call initiates a request/response conversation on a new connection
func (p *Peer) Call(method string, payload arpcdata.Encodable) (Response, error) {
	return p.CallContext(context.Background(), method, payload)
}

func (p *Peer) CallWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) (Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return p.CallContext(ctx, method, payload)
}

// CallContext performs an RPC call over a new connection
func (p *Peer) CallContext(ctx context.Context, method string, payload arpcdata.Encodable) (Response, error) {
	// Get a fresh connection for this request
	conn, err := p.getConnection(ctx)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close() // Always close after request completes

	// Apply context deadlines to the connection
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetWriteDeadline(deadline)
		conn.SetReadDeadline(deadline)
	}
	defer func() {
		conn.SetWriteDeadline(time.Time{})
		conn.SetReadDeadline(time.Time{})
	}()

	// Serialize the payload if provided
	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			return Response{}, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	// Build the RPC request and encode it
	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}
	reqBytes, err := req.Encode()
	if err != nil {
		return Response{}, fmt.Errorf("failed to encode request: %w", err)
	}

	// Write the request
	if _, err := conn.Write(reqBytes); err != nil {
		return Response{}, fmt.Errorf("failed to write request: %w", err)
	}

	// Read the response
	prefix := headerPool.Get().([]byte)
	defer headerPool.Put(prefix)

	if _, err := io.ReadFull(conn, prefix); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return Response{}, context.DeadlineExceeded
		}
		return Response{}, fmt.Errorf("failed to read length prefix: %w", err)
	}

	totalLength := binary.LittleEndian.Uint32(prefix)
	if totalLength < 4 {
		return Response{}, fmt.Errorf("invalid total length %d", totalLength)
	}

	// Allocate a buffer with exactly totalLength bytes
	buf := make([]byte, totalLength)
	copy(buf, prefix)

	// Read the remaining bytes
	if _, err := io.ReadFull(conn, buf[4:]); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return Response{}, context.DeadlineExceeded
		}
		return Response{}, fmt.Errorf("failed to read full response: %w", err)
	}

	// Decode the response
	var resp Response
	if err := resp.Decode(buf); err != nil {
		return Response{}, fmt.Errorf("failed to decode response: %w", err)
	}

	return resp, nil
}

// CallMsg performs an RPC call and returns the response data
func (p *Peer) CallMsg(ctx context.Context, method string, payload arpcdata.Encodable) ([]byte, error) {
	resp, err := p.CallContext(ctx, method, payload)
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

func (p *Peer) CallMsgWithTimeout(timeout time.Duration, method string, payload arpcdata.Encodable) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return p.CallMsg(ctx, method, payload)
}

// CallBinary performs an RPC call for file I/O-style operations
func (p *Peer) CallBinary(ctx context.Context, method string, payload arpcdata.Encodable, dst []byte) (int, error) {
	// Get a fresh connection for this request
	conn, err := p.getConnection(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close() // Always close after request completes

	// Apply context deadlines
	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return 0, fmt.Errorf("failed to set deadline: %w", err)
		}
	}
	defer func() {
		conn.SetDeadline(time.Time{})
	}()

	// Serialize the payload
	var payloadBytes []byte
	if payload != nil {
		payloadBytes, err = payload.Encode()
		if err != nil {
			return 0, fmt.Errorf("failed to encode payload: %w", err)
		}
	}

	// Build the RPC request
	req := Request{
		Method:  method,
		Payload: payloadBytes,
	}

	// Encode and send the request
	reqBytes, err := req.Encode()
	if err != nil {
		return 0, fmt.Errorf("failed to encode request: %w", err)
	}

	if _, err := conn.Write(reqBytes); err != nil {
		return 0, fmt.Errorf("failed to write request: %w", err)
	}

	// Read the response header
	headerPrefix := headerPool.Get().([]byte)
	defer headerPool.Put(headerPrefix)

	if _, err := io.ReadFull(conn, headerPrefix); err != nil {
		return 0, fmt.Errorf("failed to read header length prefix: %w", err)
	}

	headerTotalLength := binary.LittleEndian.Uint32(headerPrefix)
	if headerTotalLength < 4 {
		return 0, fmt.Errorf("invalid header length %d", headerTotalLength)
	}

	headerBuf := make([]byte, headerTotalLength)
	copy(headerBuf, headerPrefix)

	if _, err := io.ReadFull(conn, headerBuf[4:]); err != nil {
		return 0, fmt.Errorf("failed to read full header: %w", err)
	}

	var resp Response
	if err := resp.Decode(headerBuf); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	if resp.Status != 213 {
		var serErr SerializableError
		if err := serErr.Decode(resp.Data); err == nil {
			return 0, UnwrapError(serErr)
		}
		return 0, fmt.Errorf("RPC error: status %d", resp.Status)
	}

	// Receive the binary data
	return binarystream.ReceiveDataInto(conn, dst)
}

var headerPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 4)
	},
}
