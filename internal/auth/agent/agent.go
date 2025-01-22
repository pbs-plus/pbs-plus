package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sync"
	"time"

	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
)

// Agent represents an authenticated client
type Agent struct {
	config *Config
	client *http.Client
	token  string
	mu     sync.RWMutex

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Request represents a request to the server
type Request struct {
	AgentID string `json:"agent_id"`
	Data    string `json:"data,omitempty"`
}

// Response represents a response from the server
type Response struct {
	Token   string `json:"token,omitempty"`
	Message string `json:"message"`
}

// New creates a new agent instance
func New(config *Config) (*Agent, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	tlsConfig, err := config.LoadTLSConfig()
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			ForceAttemptHTTP2:     true,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		Timeout: config.Timeout,
	}

	return &Agent{
		config: config,
		client: client,
		stopCh: make(chan struct{}),
	}, nil
}

// Start initializes the agent and starts background tasks
func (a *Agent) Start(ctx context.Context) error {
	// Bootstrap to get initial token
	if err := a.bootstrap(ctx); err != nil {
		return err
	}

	// Start keep-alive if enabled
	if a.config.KeepAlive {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			a.keepAliveLoop(ctx)
		}()
	}

	return nil
}

// Stop gracefully stops the agent
func (a *Agent) Stop() {
	close(a.stopCh)
	a.wg.Wait()
}

// bootstrap performs the initial authentication
func (a *Agent) bootstrap(ctx context.Context) error {
	var lastErr error
	for attempt := 0; attempt < a.config.MaxRetries; attempt++ {
		if err := a.doBootstrap(ctx); err != nil {
			lastErr = err
			backoff := a.calculateBackoff(attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}
		return nil
	}
	return authErrors.WrapError("bootstrap", lastErr)
}

func (a *Agent) doBootstrap(ctx context.Context) error {
	req := Request{
		AgentID: a.config.AgentID,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return authErrors.WrapError("marshal_request", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		a.config.ServerURL+"/bootstrap",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return authErrors.WrapError("create_request", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(httpReq)
	if err != nil {
		return authErrors.WrapError("do_request", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return authErrors.WrapError("bootstrap",
			fmt.Errorf("unexpected status: %d", resp.StatusCode))
	}

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return authErrors.WrapError("decode_response", err)
	}

	a.mu.Lock()
	a.token = response.Token
	a.mu.Unlock()

	return nil
}

// SendRequest sends an authenticated request to the server
func (a *Agent) SendRequest(ctx context.Context, data string) (*Response, error) {
	a.mu.RLock()
	token := a.token
	a.mu.RUnlock()

	if token == "" {
		return nil, authErrors.ErrUnauthorized
	}

	req := Request{
		AgentID: a.config.AgentID,
		Data:    data,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, authErrors.WrapError("marshal_request", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST",
		a.config.ServerURL+"/secure",
		bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, authErrors.WrapError("create_request", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", token)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, authErrors.WrapError("do_request", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		// Token might be expired, try to bootstrap again
		if err := a.bootstrap(ctx); err != nil {
			return nil, err
		}
		return a.SendRequest(ctx, data) // Retry with new token
	}

	if resp.StatusCode != http.StatusOK {
		return nil, authErrors.WrapError("send_request",
			fmt.Errorf("unexpected status: %d", resp.StatusCode))
	}

	var response Response
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, authErrors.WrapError("decode_response", err)
	}

	return &response, nil
}

// keepAliveLoop maintains the connection and token validity
func (a *Agent) keepAliveLoop(ctx context.Context) {
	ticker := time.NewTicker(a.config.KeepAliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stopCh:
			return
		case <-ticker.C:
			_, err := a.SendRequest(ctx, "keepalive")
			if err != nil {
				// Log error but don't stop the loop
				// Might want to add proper logging here
				continue
			}
		}
	}
}

func (a *Agent) calculateBackoff(attempt int) time.Duration {
	backoff := float64(a.config.RetryInterval) *
		math.Pow(a.config.BackoffFactor, float64(attempt))
	if backoff > float64(a.config.MaxBackoff) {
		backoff = float64(a.config.MaxBackoff)
	}
	return time.Duration(backoff)
}

// GetToken returns the current token
func (a *Agent) GetToken() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.token
}
