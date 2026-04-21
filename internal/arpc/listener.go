package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
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
		// Should always be *tls.Conn from tls.Dialer, but handle gracefully
		_ = conn.Close()
		return nil, fmt.Errorf("unexpected connection type: %T", conn)
	}

	state := tlsConn.ConnectionState()
	syslog.L.Info().
		WithField("tls_version", state.Version).
		WithField("alpn", state.NegotiatedProtocol).
		WithMessage("TLS connection established").
		Write()
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

	// Check if listener supports deadlines (TCPListener)
	tcpListener, hasDeadline := listener.(*net.TCPListener)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Set deadline to prevent unbounded blocking on Accept
		if hasDeadline {
			tcpListener.SetDeadline(time.Now().Add(2 * time.Second))
		}

		conn, err := listener.Accept()
		if err != nil {
			if hasDeadline {
				// Check if it's a timeout error
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Timeout - check context and continue
					select {
					case <-ctx.Done():
						return ctx.Err()
					default:
						continue
					}
				}
			}
			// Check if context was cancelled during Accept
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
			defer c.Close()

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
			defer smuxS.Close()

			var reqHeaders http.Header
			stream, err := smuxS.AcceptStream()
			if err == nil {
				hdrs, rerr := readHeadersFrame(stream)
				if rerr != nil {
					syslog.L.Debug().WithMessage("failed to read headers, sending rejection").Write()
					_ = writeRejectionFrame(stream, RejectionFrame{
						Message: "failed to parse headers",
						Code:    400,
					})
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
				syslog.L.Debug().
					WithField("error", err.Error()).
					WithMessage("registration failed, sending rejection").
					Write()

				_ = writeRejectionFrame(stream, RejectionFrame{
					Message: err.Error(),
					Code:    403,
				})
				_ = stream.Close()
				return
			}

			if err := writeHeadersSuccess(stream); err != nil {
				syslog.L.Debug().WithMessage("failed to send success marker").Write()
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

			_ = pipe.Serve()
		}(conn)
	}
}

func ListenAndServe(ctx context.Context, addr string, agentsManager *AgentsManager, tlsConfig *tls.Config, router Router) error {
	listener, err := Listen(ctx, addr, tlsConfig)
	if err != nil {
		return err
	}
	defer listener.Close()

	return Serve(ctx, agentsManager, listener, router)
}
