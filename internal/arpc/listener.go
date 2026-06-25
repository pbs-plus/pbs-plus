package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/xtaci/smux"
)

func dialServerContext(ctx context.Context, serverAddr string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}

	dialer := &tls.Dialer{
		Config: tlsConfig,
	}
	conn, err := dialer.DialContext(ctx, "tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("TLS dial failed: %w", err)
	}

	tlsConn, ok := conn.(*tls.Conn)
	if !ok {
		if cerr := conn.Close(); cerr != nil {
			log.Debug("failed to close unexpected connection type")
		}
		return nil, fmt.Errorf("unexpected connection type: %T", conn)
	}

	state := tlsConn.ConnectionState()
	log.Info("tls: connection established", "alpn", state.NegotiatedProtocol, "tls_version", state.Version)

	return conn, nil
}

func Listen(ctx context.Context, addr string, tlsConfig *tls.Config) (net.Listener, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}

	arpcTls := tlsConfig.Clone()
	arpcTls.NextProtos = []string{"pbsarpc"}

	listener, err := tls.Listen("tcp", addr, arpcTls)
	if err != nil {
		return nil, fmt.Errorf("tls listen failed: %w", err)
	}

	return listener, nil
}

func Serve(ctx context.Context, agentsManager *AgentsManager, listener net.Listener, router Router) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	tcpListener, hasDeadline := listener.(*net.TCPListener)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if hasDeadline {
			_ = tcpListener.SetDeadline(time.Now().Add(2 * time.Second))
		}

		conn, err := listener.Accept()
		if err != nil {
			if hasDeadline {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						continue
					}
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer func() { _ = c.Close() }()

			tlsConn, ok := c.(*tls.Conn)
			if !ok {
				return
			}

			if err := tlsConn.Handshake(); err != nil {
				return
			}

			smuxS, err := smux.Server(c, defaultConfig())
			if err != nil {
				return
			}
			defer func() { _ = smuxS.Close() }()

			var reqHeaders http.Header
			stream, err := smuxS.AcceptStream()
			if err == nil {
				hdrs, rerr := readHeadersFrame(stream)
				if rerr != nil {
					log.Debug("failed to read headers, sending rejection")
					if rerr := writeRejectionFrame(stream, RejectionFrame{
						Message: "failed to parse headers",
						Code:    400,
					}); rerr != nil {
						log.Debug("failed to write rejection frame")
					}
					_ = stream.Close()
					return
				}
				reqHeaders = hdrs
			} else {
				return
			}

			pCtx, pCan := context.WithCancel(ctx)
			defer pCan()

			pipe, id, err := agentsManager.registerStreamPipe(pCtx, smuxS, c, reqHeaders)
			if err != nil {
				log.Debug("registration failed, sending rejection", "error", err.Error())

				if rerr := writeRejectionFrame(stream, RejectionFrame{
					Message: err.Error(),
					Code:    403,
				}); rerr != nil {
					log.Debug("failed to write rejection frame")
				}
				_ = stream.Close()
				return
			}

			if err := writeHeadersSuccess(stream); err != nil {
				log.Debug("failed to send success marker")
				_ = stream.Close()
				pipe.Close()
				agentsManager.unregisterStreamPipe(id)
				return
			}
			_ = stream.Close()

			defer func() {
				pipe.Close()
				agentsManager.unregisterStreamPipe(id)
			}()

			pipe.SetRouter(router)

			if err := pipe.Serve(); err != nil {
				log.Error(err, "arpc: pipe serve failed")
			}
		}(conn)
	}
}

func ListenAndServe(ctx context.Context, addr string, agentsManager *AgentsManager, tlsConfig *tls.Config, router Router) error {
	listener, err := Listen(ctx, addr, tlsConfig)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()

	return Serve(ctx, agentsManager, listener, router)
}
