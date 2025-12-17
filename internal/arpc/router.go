package arpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/fxamacker/cbor/v2"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils/safemap"
	"github.com/xtaci/smux"
)

type HandlerFunc func(req *Request) (Response, error)

type Router struct {
	handlers *safemap.Map[string, HandlerFunc]
}

func NewRouter() Router {
	return Router{handlers: safemap.New[string, HandlerFunc]()}
}

func (r *Router) Handle(method string, handler HandlerFunc) {
	r.handlers.Set(method, handler)
}

func (r *Router) CloseHandle(method string) {
	r.handlers.Del(method)
}

func (r *Router) serveStream(stream *smux.Stream) {
	dec := cbor.NewDecoder(stream)
	enc := cbor.NewEncoder(stream)

	var req Request
	if err := dec.Decode(&req); err != nil {
		writeErrorResponse(stream, http.StatusBadRequest, err)
		return
	}

	if req.Method == "" {
		writeErrorResponse(stream, http.StatusBadRequest, errors.New("missing method field"))
		return
	}

	handler, ok := r.handlers.Get(req.Method)
	if !ok {
		writeErrorResponse(stream, http.StatusNotFound, fmt.Errorf("method not found: %s", req.Method))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-stream.GetDieCh():
			syslog.L.Debug().WithMessage("cancelling stream due to closure itself").Write()
			cancel()
		case <-ctx.Done():
		}
	}()

	req.Context = ctx

	resp, err := handler(&req)
	if err != nil {
		writeErrorResponse(stream, http.StatusInternalServerError, err)
		return
	}

	if err = enc.Encode(resp); err != nil {
		syslog.L.Debug().WithField("req", req.Method).WithField("code", resp.Status).WithMessage("sending response")
		return
	}

	if resp.Status == 213 && resp.RawStream != nil {
		syslog.L.Debug().WithField("req", req.Method).WithMessage("sending binary")

		syncByte := make([]byte, 1)
		if _, err := stream.Read(syncByte); err != nil {
			return
		}
		if syncByte[0] != 0xFF {
			return
		}

		resp.RawStream(stream)
	}
}
