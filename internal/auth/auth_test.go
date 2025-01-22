package auth

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/auth/agent"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/certificates"
	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
	serverLib "github.com/sonroyaalmerol/pbs-plus/internal/auth/server"
	"github.com/sonroyaalmerol/pbs-plus/internal/auth/token"
	"golang.org/x/time/rate"
)

// Server represents the authentication server
type Server struct {
	config  *serverLib.Config
	limiter *rate.Limiter
	tokens  *token.Manager
	server  *http.Server
}

type Request struct {
	AgentID string `json:"agent_id"`
	Data    string `json:"data,omitempty"`
}

type Response struct {
	Token   string `json:"token,omitempty"`
	Message string `json:"message"`
}

// New creates a new Server instance with the provided configuration
func New(config *serverLib.Config) (*Server, error) {
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

	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap", s.withMiddleware(s.handleBootstrap))
	mux.HandleFunc("/secure", s.withMiddleware(s.handleSecure))

	s.server = &http.Server{
		Addr:           config.Address,
		Handler:        mux,
		TLSConfig:      tlsConfig,
		ReadTimeout:    config.ReadTimeout,
		WriteTimeout:   config.WriteTimeout,
		MaxHeaderBytes: config.MaxHeaderBytes,
	}

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	return s.server.ListenAndServeTLS(s.config.CertFile, s.config.KeyFile)
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

func TestEndToEnd(t *testing.T) {
	// Create temporary directory for test certificates
	certsDir, err := os.MkdirTemp("", "auth-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(certsDir)

	// Generate certificates
	certOpts := certificates.DefaultOptions()
	certOpts.OutputDir = certsDir
	certOpts.Organization = "Test Auth System"
	certOpts.ValidDays = 1
	certOpts.Hostnames = []string{"localhost", "test.local"}
	certOpts.IPs = []net.IP{net.ParseIP("127.0.0.1")}

	generator, err := certificates.NewGenerator(certOpts)
	if err != nil {
		t.Fatal(err)
	}

	if err := generator.GenerateAll(); err != nil {
		t.Fatal(err)
	}

	// Start server
	serverConfig := serverLib.DefaultConfig()
	serverConfig.Address = ":44443" // Different port for testing
	serverConfig.CertFile = filepath.Join(certsDir, "server.crt")
	serverConfig.KeyFile = filepath.Join(certsDir, "server.key")
	serverConfig.CAFile = filepath.Join(certsDir, "ca.crt")
	serverConfig.TokenExpiration = 1 * time.Hour

	srv, err := New(serverConfig)
	if err != nil {
		t.Fatal(err)
	}

	// Start server in goroutine
	serverErrCh := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			if !isClosedConnError(err) {
				serverErrCh <- err
			}
		}
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	defer func() {
		// Graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Stop(shutdownCtx); err != nil && !isClosedConnError(err) {
			t.Error("shutdown error:", err)
		}

		// Check if there were any server errors
		select {
		case err := <-serverErrCh:
			if err != nil {
				if !strings.Contains(err.Error(), "Server closed") {
					t.Error("server error:", err)
				}
			}
		default:
		}
	}()

	// Create and start multiple agents
	agents := make([]*agent.Agent, 3)
	for i := 0; i < 3; i++ {
		agentConfig := agent.DefaultConfig()
		agentConfig.AgentID = fmt.Sprintf("test-agent-%d", i)
		agentConfig.ServerURL = "https://localhost:44443"
		agentConfig.CertFile = filepath.Join(certsDir, "agent.crt")
		agentConfig.KeyFile = filepath.Join(certsDir, "agent.key")
		agentConfig.CAFile = filepath.Join(certsDir, "ca.crt")
		agentConfig.Timeout = 5 * time.Second
		agentConfig.MaxRetries = 3
		agentConfig.RetryInterval = 100 * time.Millisecond
		agentConfig.KeepAlive = true
		agentConfig.KeepAliveInterval = 500 * time.Millisecond

		a, err := agent.New(agentConfig)
		if err != nil {
			t.Fatal(err)
		}
		agents[i] = a
	}

	// Start all agents
	ctx := context.Background()
	for i, a := range agents {
		if err := a.Start(ctx); err != nil {
			t.Fatalf("Failed to start agent %d: %v", i, err)
		}
		defer a.Stop()
	}

	// Test parallel requests from all agents
	t.Run("ParallelRequests", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, len(agents)*3) // 3 requests per agent

		for _, a := range agents {
			wg.Add(1)
			go func(agent *agent.Agent) {
				defer wg.Done()
				for i := 0; i < 3; i++ {
					resp, err := agent.SendRequest(ctx, fmt.Sprintf("test message %d", i))
					if err != nil {
						errors <- err
						return
					}
					if resp == nil || resp.Message == "" {
						errors <- fmt.Errorf("empty response")
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}(a)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			if err != nil {
				t.Error(err)
			}
		}
	})

	// Test token expiration and renewal
	t.Run("TokenRenewal", func(t *testing.T) {
		// Get initial token
		initialToken := agents[0].GetToken()
		if initialToken == "" {
			t.Fatal("Expected non-empty initial token")
		}

		// Wait for a keepalive cycle
		time.Sleep(600 * time.Millisecond)

		// Send another request
		resp, err := agents[0].SendRequest(ctx, "test token renewal")
		if err != nil {
			t.Fatal(err)
		}
		if resp == nil {
			t.Fatal("Expected non-nil response")
		}
	})

	// Test invalid requests
	t.Run("InvalidRequests", func(t *testing.T) {
		// Load client certificate for invalid token test
		cert, err := tls.LoadX509KeyPair(
			filepath.Join(certsDir, "agent.crt"),
			filepath.Join(certsDir, "agent.key"),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Load CA cert
		caCert, err := os.ReadFile(filepath.Join(certsDir, "ca.crt"))
		if err != nil {
			t.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			t.Fatal("Failed to append CA cert")
		}

		// Create client with valid certificates but invalid token
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					Certificates: []tls.Certificate{cert},
					RootCAs:      caCertPool,
				},
			},
		}

		reqBody := []byte(`{"agent_id": "test-invalid", "data": "test"}`)
		req, err := http.NewRequest("POST",
			"https://localhost:44443/secure",
			bytes.NewBuffer(reqBody))
		if err != nil {
			t.Fatal(err)
		}

		req.Header.Set("Authorization", "invalid-token")
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("Expected status unauthorized, got %v", resp.Status)
		}
	})

	// End of test
}

// Helper function to check for "use of closed network connection" error
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "use of closed network connection")
}
