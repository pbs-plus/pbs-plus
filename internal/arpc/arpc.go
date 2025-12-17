package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

func dialServer(serverAddr string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}

	conn, err := tls.Dial("tcp", serverAddr, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("TLS dial failed: %w", err)
	}

	state := conn.ConnectionState()
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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}

		go func(c net.Conn) {
			tlsConn, ok := c.(*tls.Conn)
			if !ok {
				c.Close()
				return
			}

			if err := tlsConn.Handshake(); err != nil {
				c.Close()
				return
			}

			if len(tlsConn.ConnectionState().PeerCertificates) == 0 {
				c.Close()
				return
			}

			smuxS, err := smux.Server(c, smux.DefaultConfig())
			if err != nil {
				c.Close()
				return
			}

			var reqHeaders http.Header
			stream, err := smuxS.AcceptStream()
			if err == nil {
				hdrs, rerr := readHeadersFrame(stream)
				if rerr != nil {
					syslog.L.Debug().WithMessage("closing tun and conn due to missing header frames").Write()
					_ = stream.Close()
					_ = smuxS.Close()
					_ = c.Close()
					return
				}
				reqHeaders = hdrs
				_ = stream.Close()
			}

			pCtx, pCan := context.WithCancel(ctx)
			defer pCan()

			pipe, id, err := agentsManager.registerStreamPipe(pCtx, smuxS, c, reqHeaders)
			if err != nil {
				syslog.L.Debug().WithMessage("closing tun and conn due to stream pipe err reg").Write()
				_ = smuxS.Close()
				_ = c.Close()
				return
			}

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
