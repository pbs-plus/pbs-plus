package arpc

import "net/http"

// Session is the common interface for ARPC transport pipes.
// Both StreamPipe (TCP/mTLS/smux) and QuicPipe (QUIC) implement it.
type Session interface {
	SetRouter(Router)
	SetHeaders(http.Header)
	Serve() error
	Close()
	GetState() ConnectionState
}
