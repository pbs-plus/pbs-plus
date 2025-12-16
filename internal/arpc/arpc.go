package arpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/xtaci/smux"
)

func dialServer(serverAddr string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		return nil, fmt.Errorf("missing tls config")
	}

	dialer := &net.Dialer{
		Timeout: 15 * time.Second,
	}

	conn, err := tls.DialWithDialer(dialer, "tcp", serverAddr, tlsConfig)
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

func ConnectToServer(ctx context.Context, serverAddr string, headers http.Header, tlsConfig *tls.Config) (*StreamPipe, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if tlsConfig == nil || len(tlsConfig.Certificates) == 0 {
		return nil, fmt.Errorf("TLS configuration must include client certificate")
	}

	arpcTls := tlsConfig.Clone()
	arpcTls.NextProtos = []string{"pbsarpc"}

	conn, err := dialServer(serverAddr, arpcTls)
	if err != nil {
		syslog.L.Info().WithField("NextProtos", arpcTls.NextProtos).WithField("serverAddr", serverAddr).Write()
		return nil, fmt.Errorf("server not reachable (%s): %w", serverAddr, err)
	}

	smuxConfig := smux.DefaultConfig()
	session, err := smux.Client(conn, smuxConfig)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create smux client: %w", err)
	}

	pipe, err := NewStreamPipe(session, conn)
	if err != nil {
		_ = session.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to create pipe: %w", err)
	}

	if headers == nil {
		headers = make(http.Header)
	}

	headers.Add("ARPCVersion", "2")

	pipe.tlsConfig = arpcTls
	pipe.serverAddr = serverAddr
	pipe.headers = headers

	stream, err := pipe.OpenStream()
	if err != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to initialize header stream: %w", err)
	}
	if werr := writeHeadersFrame(stream, headers); werr != nil {
		_ = stream.Close()
		pipe.Close()
		return nil, fmt.Errorf("failed to write headers: %w", werr)
	}
	if cerr := stream.Close(); cerr != nil {
		pipe.Close()
		return nil, fmt.Errorf("failed to close header stream: %w", cerr)
	}

	return pipe, nil
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

			smuxConfig := smux.DefaultConfig()
			session, err := smux.Server(c, smuxConfig)
			if err != nil {
				c.Close()
				return
			}

			var reqHeaders http.Header
			stream, err := session.AcceptStream()
			if err == nil {
				if hdrs, rerr := readHeadersFrame(stream); rerr == nil {
					reqHeaders = hdrs
				}
				_ = stream.Close()
			}

			pipe, id, err := agentsManager.createStreamPipe(session, c, reqHeaders)
			if err != nil {
				_ = session.Close()
				_ = c.Close()
				return
			}

			defer func() {
				pipe.Close()
				agentsManager.closeStreamPipe(id)
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
