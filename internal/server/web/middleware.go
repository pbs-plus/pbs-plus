package web

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/conf"
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

type visitor struct {
	mu       sync.Mutex
	tokens   float64
	maxBurst float64
	rate     float64
	lastTime time.Time
}

type rateLimiter struct {
	mu       sync.Mutex
	visitors map[string]*visitor
	rate     float64
	burst    float64
}

func newRateLimiter(rate, burst float64) *rateLimiter {
	rl := &rateLimiter{
		visitors: make(map[string]*visitor),
		rate:     rate,
		burst:    burst,
	}
	go rl.cleanup()
	return rl
}

func (rl *rateLimiter) allow(ip string) bool {
	rl.mu.Lock()
	v, ok := rl.visitors[ip]
	if !ok {
		v = &visitor{tokens: rl.burst, maxBurst: rl.burst, rate: rl.rate, lastTime: time.Now()}
		rl.visitors[ip] = v
	}
	rl.mu.Unlock()

	v.mu.Lock()
	defer v.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(v.lastTime)
	v.lastTime = now
	v.tokens += elapsed.Seconds() * v.rate
	if v.tokens > v.maxBurst {
		v.tokens = v.maxBurst
	}
	if v.tokens < 1 {
		return false
	}
	v.tokens--
	return true
}

func (rl *rateLimiter) cleanup() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		rl.mu.Lock()
		cutoff := time.Now().Add(-10 * time.Minute)
		for ip, v := range rl.visitors {
			v.mu.Lock()
			if v.lastTime.Before(cutoff) {
				delete(rl.visitors, ip)
			}
			v.mu.Unlock()
		}
		rl.mu.Unlock()
	}
}

var globalRateLimiter = newRateLimiter(conf.HTTPRateLimit, conf.HTTPRateBurst)

func RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !globalRateLimiter.allow(ip) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-XSS-Protection", "0")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Pragma", "no-cache")
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
