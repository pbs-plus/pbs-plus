package auth

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt"
)

type Server struct {
	tokens     map[string]bool
	tokenMutex sync.RWMutex
}

const (
	jwtSecret = "your-secret-key" // In production, use a secure secret
	certFile  = "server.crt"
	keyFile   = "server.key"
	caFile    = "ca.crt"
)

func NewServer() *Server {
	return &Server{
		tokens: make(map[string]bool),
	}
}

func (s *Server) generateToken(agentID string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"agent_id": agentID,
		"exp":      time.Now().Add(time.Hour * 24).Unix(),
	})
	return token.SignedString([]byte(jwtSecret))
}

func (s *Server) validateToken(tokenString string) bool {
	s.tokenMutex.RLock()
	defer s.tokenMutex.RUnlock()
	return s.tokens[tokenString]
}

func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	token, err := s.generateToken(req.AgentID)
	if err != nil {
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	s.tokenMutex.Lock()
	s.tokens[token] = true
	s.tokenMutex.Unlock()

	resp := AgentResponse{
		Token:   token,
		Message: "Bootstrap successful",
	}

	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSecureEndpoint(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if token == "" || !s.validateToken(token) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	var req AgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	resp := AgentResponse{
		Message: "Received data: " + req.Data,
	}

	json.NewEncoder(w).Encode(resp)
}
