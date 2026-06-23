//go:build linux

package web

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type contextKey int

const (
	requestIDKey contextKey = iota
)

func GetRequestID(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New().String()
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			duration := time.Since(start)

			logger.Debug("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", duration.Milliseconds(),
				"request_id", GetRequestID(r.Context()),
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger := slog.Default()
				reqID := GetRequestID(r.Context())
				logger.Error("panic recovered",
					"error", fmt.Sprintf("%v", err),
					"request_id", reqID,
					"stack", string(debug.Stack()),
					"path", r.URL.Path,
					"method", r.Method,
				)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func RequireAgentAuth(st *store.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hostname, err := checkAgentAuth(st, r)
			if err != nil {
				syslog.L.Error(err).
					WithField("mode", "agent_only").
					WithField("hostname", getClientInfo(r)).
					WithField("request_id", GetRequestID(r.Context())).
					Write()
				http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
				return
			}
			r.Header.Set("X-PBS-Authenticated-Agent", hostname)
			next.ServeHTTP(w, r)
		})
	}
}

func RequireServerAuth(st *store.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if err := checkProxyAuth(r); err != nil && !IsLocalhost(r) {
				syslog.L.Error(err).
					WithField("mode", "server_only").
					WithField("hostname", getClientInfo(r)).
					WithField("request_id", GetRequestID(r.Context())).
					Write()
				http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func RequireAgentOrServerAuth(st *store.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authenticated := false
			var lastErr error

			hostname, agentErr := checkAgentAuth(st, r)
			if agentErr == nil {
				authenticated = true
			} else {
				lastErr = agentErr
			}

			if err := checkProxyAuth(r); err == nil || IsLocalhost(r) {
				authenticated = true
			} else {
				lastErr = err
			}

			if !authenticated {
				syslog.L.Error(lastErr).
					WithField("mode", "agent_or_server").
					WithField("hostname", getClientInfo(r)).
					WithField("request_id", GetRequestID(r.Context())).
					Write()
				http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
				return
			}
			if hostname != "" {
				r.Header.Set("X-PBS-Authenticated-Agent", hostname)
			}
			next.ServeHTTP(w, r)
		})
	}
}

// NOTE: Rate limiting middleware should be inserted here in the chain,
// between Recovery and Auth middleware. The order is:
//   RequestID → RequestLogger → Recovery → RateLimiter → Auth
// A simple token-bucket or golang.org/x/time/rate based limiter
