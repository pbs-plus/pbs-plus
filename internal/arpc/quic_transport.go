package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/quic-go/quic-go"
)

var quicNextProtos = []string{"pbsarpc-quic"}

func quicConfig() *quic.Config {
	return &quic.Config{
		MaxIdleTimeout:             5 * time.Minute,
		KeepAlivePeriod:            30 * time.Second,
		HandshakeIdleTimeout:       10 * time.Second,
		MaxIncomingStreams:         1 << 16,
		MaxStreamReceiveWindow:     10 * 1024 * 1024,
		MaxConnectionReceiveWindow: 15 * 1024 * 1024,
	}
}

// QuicPipe is the QUIC-based transport for the ARPC control plane.
// Binary data streams use TCP via the data Pipe.
type QuicPipe struct {
	mu     sync.RWMutex
	conn   *quic.Conn
	router *Router

	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header
	version    string

	cborEnc cbor.EncMode
	cborDec cbor.DecMode

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewQuicServerPipe(ctx context.Context, conn *quic.Conn) *QuicPipe {
	ctx, cancel := context.WithCancel(ctx)
	enc, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		syslog.L.Error(err).WithMessage("arpc: init cbor encoder").Write()
	}
	dec, err := cbor.DecOptions{MaxArrayElements: math.MaxInt32}.DecMode()
	if err != nil {
		dec, err = cbor.DecOptions{}.DecMode()
		if err != nil {
			syslog.L.Error(err).WithMessage("arpc: init cbor decoder").Write()
		}
	}
	return &QuicPipe{
		ctx:        ctx,
		cancelFunc: cancel,
		conn:       conn,
		cborEnc:    enc,
		cborDec:    dec,
	}
}

func DialQuic(ctx context.Context, serverAddr string, tlsConfig *tls.Config, headers http.Header) (*QuicPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("TLS configuration must include client certificate")
	}

	quicTLS := tlsConfig.Clone()
	quicTLS.NextProtos = quicNextProtos

	conn, err := quic.DialAddr(ctx, serverAddr, quicTLS, quicConfig())
	if err != nil {
		syslog.L.Error(err).WithField("serverAddr", serverAddr).Write()
		return nil, fmt.Errorf("QUIC dial failed (%s): %w", serverAddr, err)
	}

	syslog.L.Info().
		WithField("quic_version", conn.ConnectionState().Version).
		WithMessage("quic: connection established").
		Write()

	// Copy headers to avoid mutating the caller's map (agent reuses it across
	// reconnects, so Add would accumulate duplicate values).
	hdrCopy := make(http.Header, len(headers)+1)
	maps.Copy(hdrCopy, headers)
	hdrCopy.Set("ARPCVersion", "2")

	pipeCtx, pipeCancel := context.WithCancel(ctx)
	enc, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		syslog.L.Error(err).WithMessage("arpc: init cbor encoder").Write()
	}
	dec, err := cbor.DecOptions{MaxArrayElements: math.MaxInt32}.DecMode()
	if err != nil {
		dec, err = cbor.DecOptions{}.DecMode()
		if err != nil {
			syslog.L.Error(err).WithMessage("arpc: init cbor decoder").Write()
		}
	}

	pipe := &QuicPipe{
		ctx:        pipeCtx,
		cancelFunc: pipeCancel,
		conn:       conn,
		serverAddr: serverAddr,
		tlsConfig:  quicTLS,
		version:    "2",
		headers:    hdrCopy,
		cborEnc:    enc,
		cborDec:    dec,
	}

	stream, err := pipe.OpenStream()
	if err != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to initialize header stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	if werr := writeHeadersFrame(stream, hdrCopy); werr != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}

	if err := readHandshakeResponse(stream); err != nil {
		pipe.Close()
		return nil, err
	}

	return pipe, nil
}

func (q *QuicPipe) SetRouter(router Router) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.router = &router
}

func (q *QuicPipe) GetRouter() *Router {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.router
}

func (q *QuicPipe) GetVersion() string {
	return q.version
}

func (q *QuicPipe) SetHeaders(headers http.Header) {
	q.headers = headers
}

func (q *QuicPipe) OpenStream() (ARPCStream, error) {
	return q.conn.OpenStreamSync(q.ctx)
}

func (q *QuicPipe) Close() {
	q.cancelFunc()
	if q.conn != nil {
		_ = q.conn.CloseWithError(0, "pipe closed")
	}
}

func (q *QuicPipe) Serve() error {
	for {
		stream, err := q.conn.AcceptStream(q.ctx)
		if err != nil {
			return err
		}

		q.mu.RLock()
		router := q.GetRouter()
		q.mu.RUnlock()

		if router == nil {
			_ = stream.Close()
			continue
		}

		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					syslog.L.Debug().
						WithField("panic", fmt.Sprintf("%v", rec)).
						WithMessage("recovered from panic in quic handler").
						Write()
				}
				_ = stream.Close()
			}()
			router.serveStream(stream)
		}()
	}
}

func (q *QuicPipe) call(ctx context.Context, method string, payload any) (ARPCStream, *Response, error) {
	stream, err := q.OpenStream()
	if err != nil {
		return nil, nil, err
	}

	enc := q.cborEnc.NewEncoder(stream)
	dec := q.cborDec.NewDecoder(stream)

	if deadline, ok := ctx.Deadline(); ok {
		if err := stream.SetDeadline(deadline); err != nil {
			fmt.Printf("arpc: failed to set stream deadline: %v\n", err)
		}
	}

	var payloadBytes []byte
	if payload != nil {
		switch p := payload.(type) {
		case []byte:
			payloadBytes = p
		default:
			payloadBytes, err = q.cborEnc.Marshal(p)
			if err != nil {
				return stream, nil, fmt.Errorf("marshal payload: %w", err)
			}
		}
	}

	req := Request{Method: method, Payload: payloadBytes, Headers: q.headers}
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

func (q *QuicPipe) checkRPCError(resp *Response) error {
	if resp.Status != http.StatusOK {
		if len(resp.Data) > 0 {
			var serErr SerializableError
			if err := q.cborDec.Unmarshal(resp.Data, &serErr); err == nil {
				return UnwrapError(serErr)
			}
		}
		return fmt.Errorf("RPC error: %s (status %d)", resp.Message, resp.Status)
	}
	return nil
}

func (q *QuicPipe) Call(ctx context.Context, method string, payload any, out any) error {
	stream, resp, err := q.call(ctx, method, payload)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	if resp.Status == StatusRawStream {
		handler, ok := out.(RawStreamHandler)
		if !ok || handler == nil {
			return fmt.Errorf("invalid out handler while in raw stream mode")
		}

		if err := performHandshake(stream); err != nil {
			return err
		}

		return handler(stream)
	}

	if err := q.checkRPCError(resp); err != nil {
		return err
	}

	if out == nil || len(resp.Data) == 0 {
		return nil
	}
	switch dst := out.(type) {
	case *[]byte:
		*dst = append((*dst)[:0], resp.Data...)
		return nil
	default:
		return q.cborDec.Unmarshal(resp.Data, out)
	}
}

func (q *QuicPipe) CallData(ctx context.Context, method string, payload any) ([]byte, error) {
	var out []byte
	if err := q.Call(ctx, method, payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (q *QuicPipe) CallMessage(ctx context.Context, method string, payload any) (string, error) {
	stream, resp, err := q.call(ctx, method, payload)
	if err != nil {
		return "", err
	}
	defer func() { _ = stream.Close() }()

	if resp.Status == StatusRawStream {
		return "", fmt.Errorf("RPC error: raw stream not supported by CallMessage (status %d)", StatusRawStream)
	}

	if err := q.checkRPCError(resp); err != nil {
		return "", err
	}

	return resp.Message, nil
}

func (q *QuicPipe) GetState() ConnectionState {
	if q.conn == nil {
		return StateDisconnected
	}
	select {
	case <-q.conn.Context().Done():
		return StateDisconnected
	default:
	}
	if q.ctx.Err() != nil {
		return StateDisconnected
	}
	return StateConnected
}

func ListenQuic(addr string, tlsConfig *tls.Config) (*quic.Listener, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}

	quicTLS := tlsConfig.Clone()
	quicTLS.NextProtos = quicNextProtos

	if quicTLS.GetConfigForClient != nil {
		origGetConfig := quicTLS.GetConfigForClient
		quicTLS.GetConfigForClient = func(info *tls.ClientHelloInfo) (*tls.Config, error) {
			cfg, err := origGetConfig(info)
			if err != nil {
				return nil, err
			}
			cfg.NextProtos = quicNextProtos
			return cfg, nil
		}
	}

	listener, err := quic.ListenAddr(addr, quicTLS, quicConfig())
	if err != nil {
		return nil, fmt.Errorf("QUIC listen: %w", err)
	}

	return listener, nil
}

func ServeQuic(ctx context.Context, agentsManager *AgentsManager, listener *quic.Listener, router Router) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := listener.Accept(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}

		wg.Add(1)
		go func(c *quic.Conn) {
			defer wg.Done()
			defer func() { _ = c.CloseWithError(0, "done") }()

			tlsState := c.ConnectionState().TLS

			reqHeaders, err := readHeadersFromFirstStream(ctx, c)
			if err != nil {
				syslog.L.Error(err).
					WithMessage("QUIC: failed to read headers").
					Write()
				return
			}

			pCtx, pCan := context.WithCancel(ctx)
			defer pCan()

			sessionID, err := agentsManager.registerQuicPipe(pCtx, c, &tlsState, reqHeaders)
			if err != nil {
				syslog.L.Error(err).
					WithMessage("QUIC: registration failed").
					Write()
				return
			}

			qPipe := NewQuicServerPipe(pCtx, c)
			defer func() {
				qPipe.Close()
				agentsManager.unregisterQuicPipe(sessionID)
			}()

			qPipe.SetRouter(router)
			if err := qPipe.Serve(); err != nil {
				syslog.L.Error(err).WithMessage("arpc: quic pipe serve failed").Write()
			}
		}(conn)
	}
}

func ListenAndServeQuic(ctx context.Context, addr string, agentsManager *AgentsManager, tlsConfig *tls.Config, router Router) error {
	listener, err := ListenQuic(addr, tlsConfig)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()

	return ServeQuic(ctx, agentsManager, listener, router)
}

func readHeadersFromFirstStream(ctx context.Context, conn *quic.Conn) (http.Header, error) {
	headerCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	stream, err := conn.AcceptStream(headerCtx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stream.Close() }()

	hdrs, rerr := readHeadersFrame(stream)
	if rerr != nil {
		if rerr := writeRejectionFrame(stream, RejectionFrame{
			Message: "failed to parse headers",
			Code:    400,
		}); rerr != nil {
			syslog.L.Debug().WithMessage("failed to write rejection frame").Write()
		}
		return nil, rerr
	}

	if err := writeHeadersSuccess(stream); err != nil {
		return nil, err
	}

	return hdrs, nil
}
