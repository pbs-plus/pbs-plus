package testhelpers

import (
	"context"
	"encoding/json"
	"net"
	"net/http"

	tls "github.com/secure-for-ai/goktls"

	authErrors "github.com/pbs-plus/pbs-plus/internal/auth/errors"
	serverLib "github.com/pbs-plus/pbs-plus/internal/auth/server"
	"github.com/pbs-plus/pbs-plus/internal/auth/token"
	"golang.org/x/time/rate"
)

// Server represents the authentication server
type Server struct {
	config   *serverLib.Config
	limiter  *rate.Limiter
	tokens   *token.Manager
	server   *http.Server
	listener net.Listener
}

// New creates a new Server instance with the provided configuration
func NewServer(config *serverLib.Config) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// Initialize token manager
	tokenManager, err := token.NewManager(token.Config{
		TokenExpiration: config.TokenExpiration,
		SecretKey:       config.TokenSecret,
	})
	if err != nil {
		return nil, authErrors.WrapError("new_server", err)
	}

	// Initialize rate limiter
	limiter := rate.NewLimiter(rate.Limit(config.RateLimit), config.RateBurst)

	s := &Server{
		config:  config,
		limiter: limiter,
		tokens:  tokenManager,
	}

	// Setup HTTP server
	tlsConfig, err := config.LoadTLSConfig()
	if err != nil {
		return nil, err
	}

	ln, err := tls.Listen("tcp", config.Address, tlsConfig)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap", s.withMiddleware(s.handleBootstrap))
	mux.HandleFunc("/secure", s.withMiddleware(s.handleSecure))

	s.server = &http.Server{
		Addr:           config.Address,
		Handler:        mux,
		ReadTimeout:    config.ReadTimeout,
		WriteTimeout:   config.WriteTimeout,
		MaxHeaderBytes: config.MaxHeaderBytes,
	}

	s.listener = ln

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	return s.server.Serve(s.listener)
}

// Stop gracefully stops the server
func (s *Server) Stop(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// Middleware chain
func (s *Server) withMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return s.withRateLimit(
		s.withRecovery(handler),
	)
}

// Middleware: Rate limiting
func (s *Server) withRateLimit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.limiter.Allow() {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}

// Middleware: Panic recovery
func (s *Server) withRecovery(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next(w, r)
	}
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	token, err := s.tokens.GenerateToken()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(Response{
		Token:   token,
		Message: "bootstrap successful",
	})
}

func (s *Server) handleSecure(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}

	err := s.tokens.ValidateToken(token)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	json.NewEncoder(w).Encode(Response{
		Message: "secure endpoint response",
	})
}
