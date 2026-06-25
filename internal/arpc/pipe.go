package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/xtaci/smux"
)

type StreamPipe struct {
	mu     sync.RWMutex
	tun    *smux.Session
	conn   net.Conn
	router *Router

	serverAddr string
	tlsConfig  *tls.Config
	headers    http.Header
	version    string

	ctx        context.Context
	cancelFunc context.CancelFunc
	cborEnc    cbor.EncMode
	cborDec    cbor.DecMode
}

type ConnectionState int32

const (
	StateConnected ConnectionState = iota
	StateDisconnected
)

func (s *StreamPipe) SetRouter(router Router) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.router = &router
}

func (s *StreamPipe) GetRouter() *Router {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.router
}

func (s *StreamPipe) GetVersion() string {
	return s.version
}

func (s *StreamPipe) SetHeaders(headers http.Header) {
	s.headers = headers
}

func ConnectToServer(ctx context.Context, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*StreamPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("TLS configuration must include client certificate")
	}

	arpcTls := tlsConfig.Clone()
	arpcTls.NextProtos = []string{"pbsarpc"}

	conn, err := dialServerContext(ctx, serverAddr, arpcTls)
	if err != nil {
		log.Info("", "serverAddr", serverAddr, "NextProtos", arpcTls.NextProtos)
		return nil, fmt.Errorf("server not reachable (%s): %w", serverAddr, err)
	}

	smuxC, err := smux.Client(conn, defaultConfig())
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create smux client: %w", err)
	}

	pipe, err := newStreamPipe(ctx, smuxC, conn, serverAddr, arpcTls)
	if err != nil {
		log.Debug("closing tun and conn due to stream pipe err init")
		_ = smuxC.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create pipe: %w", err)
	}

	if headers == nil {
		headers = make(http.Header)
	}

	headers.Add("ARPCVersion", "2")
	pipe.headers = headers
	pipe.version = "2"

	stream, err := pipe.OpenStream()
	if err != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to initialize header stream: %w", err)
	}
	defer func() { _ = stream.Close() }()

	if werr := writeHeadersFrame(stream, headers); werr != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}

	if err := readHandshakeResponse(stream); err != nil {
		pipe.Close()
		return nil, err
	}

	return pipe, nil
}

func AcceptConnection(ctx context.Context, tun *smux.Session, conn net.Conn) (*StreamPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pipe, err := newStreamPipe(ctx, tun, conn, "", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create server pipe: %w", err)
	}

	return pipe, nil
}

func newStreamPipe(ctx context.Context, tun *smux.Session, conn net.Conn, serverAddr string, tlsConfig *tls.Config) (*StreamPipe, error) {
	if tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}

	ctx, cancel := context.WithCancel(ctx)
	cborDec, err := cbor.DecOptions{
		MaxArrayElements: math.MaxInt32,
	}.DecMode()
	if err != nil {
		cborDec, err = cbor.DecOptions{}.DecMode()
		if err != nil {
			cancel()
			return nil, fmt.Errorf("arpc: init cbor decoder: %w", err)
		}
	}

	cborEnc, err := cbor.EncOptions{}.EncMode()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("arpc: init cbor encoder: %w", err)
	}

	pipe := &StreamPipe{
		ctx:        ctx,
		cancelFunc: cancel,
		tun:        tun,
		conn:       conn,
		serverAddr: serverAddr,
		tlsConfig:  tlsConfig,

		cborDec: cborDec,
		cborEnc: cborEnc,
	}

	go func() {
		select {
		case <-tun.CloseChan():
			cancel()
		case <-ctx.Done():
		}
	}()

	return pipe, nil
}

func (s *StreamPipe) CloseChan() <-chan struct{} {
	return s.tun.CloseChan()
}

func (s *StreamPipe) OpenStream() (*smux.Stream, error) {
	if s.tun == nil {
		return nil, fmt.Errorf("nil smux tunnel")
	}
	return s.tun.OpenStream()
}

func (s *StreamPipe) Serve() error {
	if s.tun == nil {
		return fmt.Errorf("nil smux tunnel")
	}

	for {
		select {
		case <-s.ctx.Done():
			log.Debug("closing pipe due to context cancellation")
			return s.ctx.Err()
		default:
		}

		stream, err := s.tun.AcceptStream()
		if err != nil {
			if s.tun.IsClosed() {
				log.Debug("closing pipe due to closed tun")
				return fmt.Errorf("session closed: %w", err)
			}
			return err
		}

		s.mu.RLock()
		router := s.GetRouter()
		s.mu.RUnlock()

		if router == nil {
			log.Debug("closing stream due to invalid router")
			_ = stream.Close()
			continue
		}

		go func(st *smux.Stream) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Debug("recovered from panic in handler", "panic", fmt.Sprintf("%v", rec))

				}
				_ = stream.Close()
			}()
			router.serveStream(st)
		}(stream)
	}
}

func (s *StreamPipe) Close() {
	s.cancelFunc()

	if s.tun != nil && !s.tun.IsClosed() {
		log.Debug("closing tunnel due to pipe close")
		_ = s.tun.Close()
	}

	if s.conn != nil {
		log.Debug("closing conn due to pipe close")
		_ = s.conn.Close()
	}
}

func (s *StreamPipe) GetState() ConnectionState {
	if s.tun == nil || s.tun.IsClosed() {
		return StateDisconnected
	}
	if s.ctx.Err() != nil {
		return StateDisconnected
	}
	return StateConnected
}
