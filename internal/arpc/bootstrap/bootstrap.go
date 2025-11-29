package bootstrap

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pbs-plus/pbs-plus/internal/arpc"
	"github.com/pbs-plus/pbs-plus/internal/arpc/transport/wireguard"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

type BootstrapToken struct {
	Token      string    `json:"token"`
	ExpiresAt  time.Time `json:"expires_at"`
	MultiUse   bool      `json:"multi_use"`
	UsageCount int       `json:"usage_count"`
	MaxUses    int       `json:"max_uses,omitempty"` // 0 means unlimited for multi-use
}

type AgentConfig struct {
	AgentID       string    `json:"agent_id"`
	PrivateKey    string    `json:"private_key"`
	PublicKey     string    `json:"public_key"`
	AssignedIP    string    `json:"assigned_ip"`
	ServerPubKey  string    `json:"server_public_key"`
	ServerAddress string    `json:"server_address"`
	ServerPort    int       `json:"server_port"`
	AllowedIPs    []string  `json:"allowed_ips"`
	EnrolledAt    time.Time `json:"enrolled_at"`
}

type ServerState struct {
	ServerPrivateKey string                     `json:"server_private_key"`
	ServerPublicKey  string                     `json:"server_public_key"`
	ServerAddress    string                     `json:"server_address"`
	ServerPort       int                        `json:"server_port"`
	NetworkPrefix    string                     `json:"network_prefix"`
	Agents           map[string]*AgentConfig    `json:"agents"`
	Tokens           map[string]*BootstrapToken `json:"tokens"`
}

type ServerBootstrap struct {
	serverPrivateKey string
	serverPublicKey  string
	serverAddress    string
	serverPort       int
	networkPrefix    netip.Prefix

	tokens  map[string]*BootstrapToken // token -> BootstrapToken
	agents  map[string]*AgentConfig    // agentID -> AgentConfig
	usedIPs map[string]bool            // track assigned IPs

	stateFile string

	// For dynamic peer updates
	activeNode          *arpc.Node
	peerChangeListeners []chan struct{}

	mu sync.RWMutex
}

func NewServerBootstrap(serverAddress string, serverPort int, networkPrefix netip.Prefix, stateFile string) (*ServerBootstrap, error) {
	sb := &ServerBootstrap{
		serverAddress:       serverAddress,
		serverPort:          serverPort,
		networkPrefix:       networkPrefix,
		tokens:              make(map[string]*BootstrapToken),
		agents:              make(map[string]*AgentConfig),
		usedIPs:             make(map[string]bool),
		stateFile:           stateFile,
		peerChangeListeners: make([]chan struct{}, 0),
	}

	// Try to load existing state
	if err := sb.LoadState(); err != nil {
		// No existing state, generate new server keys
		privKey, err := wgtypes.GeneratePrivateKey()
		if err != nil {
			return nil, fmt.Errorf("failed to generate server key: %w", err)
		}

		pubKey := privKey.PublicKey()
		sb.serverPrivateKey = hex.EncodeToString(privKey[:])
		sb.serverPublicKey = hex.EncodeToString(pubKey[:])

		// Mark server IP as used
		serverIP := networkPrefix.Addr().String()
		sb.usedIPs[serverIP] = true

		// Save initial state
		if err := sb.SaveState(); err != nil {
			return nil, fmt.Errorf("failed to save initial state: %w", err)
		}
	}

	return sb, nil
}

func (sb *ServerBootstrap) LoadState() error {
	data, err := os.ReadFile(sb.stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return err // File doesn't exist, will create new state
		}
		return fmt.Errorf("failed to read state file: %w", err)
	}

	var state ServerState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// Restore state
	sb.serverPrivateKey = state.ServerPrivateKey
	sb.serverPublicKey = state.ServerPublicKey
	sb.serverAddress = state.ServerAddress
	sb.serverPort = state.ServerPort

	// Restore network prefix if saved
	if state.NetworkPrefix != "" {
		prefix, err := netip.ParsePrefix(state.NetworkPrefix)
		if err != nil {
			return fmt.Errorf("invalid saved network prefix: %w", err)
		}
		sb.networkPrefix = prefix
	}

	// Restore agents
	if state.Agents != nil {
		sb.agents = state.Agents

		// Rebuild usedIPs from agents
		for _, agent := range state.Agents {
			sb.usedIPs[agent.AssignedIP] = true
		}
	}

	// Mark server IP as used
	serverIP := sb.networkPrefix.Addr().String()
	sb.usedIPs[serverIP] = true

	// Restore active tokens (remove expired ones)
	if state.Tokens != nil {
		now := time.Now()
		for token, bt := range state.Tokens {
			if bt.ExpiresAt.After(now) {
				sb.tokens[token] = bt
			}
		}
	}

	return nil
}

func (sb *ServerBootstrap) SaveState() error {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	state := ServerState{
		ServerPrivateKey: sb.serverPrivateKey,
		ServerPublicKey:  sb.serverPublicKey,
		ServerAddress:    sb.serverAddress,
		ServerPort:       sb.serverPort,
		NetworkPrefix:    sb.networkPrefix.String(),
		Agents:           sb.agents,
		Tokens:           sb.tokens,
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Write to temp file first, then rename for atomicity
	tempFile := sb.stateFile + ".tmp"
	if err := os.WriteFile(tempFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	if err := os.Rename(tempFile, sb.stateFile); err != nil {
		return fmt.Errorf("failed to rename state file: %w", err)
	}

	return nil
}

func (sb *ServerBootstrap) notifyPeerChange() {
	for _, ch := range sb.peerChangeListeners {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func (sb *ServerBootstrap) RegisterExistingAgent(config *AgentConfig) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	ip, err := netip.ParseAddr(config.AssignedIP)
	if err != nil {
		return fmt.Errorf("invalid IP address: %w", err)
	}

	if !sb.networkPrefix.Contains(ip) {
		return fmt.Errorf("IP %s is not within network %s", config.AssignedIP, sb.networkPrefix)
	}

	if sb.usedIPs[config.AssignedIP] {
		return fmt.Errorf("IP %s is already in use", config.AssignedIP)
	}

	if _, exists := sb.agents[config.AgentID]; exists {
		return fmt.Errorf("agent ID %s already exists", config.AgentID)
	}

	sb.agents[config.AgentID] = config
	sb.usedIPs[config.AssignedIP] = true

	if err := sb.SaveState(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	sb.notifyPeerChange()

	return nil
}

func (sb *ServerBootstrap) MarkIPUsed(ip string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return fmt.Errorf("invalid IP address: %w", err)
	}

	if !sb.networkPrefix.Contains(addr) {
		return fmt.Errorf("IP %s is not within network %s", ip, sb.networkPrefix)
	}

	if sb.usedIPs[ip] {
		return fmt.Errorf("IP %s is already marked as used", ip)
	}

	sb.usedIPs[ip] = true
	return nil
}

func (sb *ServerBootstrap) ReleaseIP(ip string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if ip == sb.networkPrefix.Addr().String() {
		return fmt.Errorf("cannot release server IP")
	}

	if !sb.usedIPs[ip] {
		return fmt.Errorf("IP %s is not marked as used", ip)
	}

	delete(sb.usedIPs, ip)
	return nil
}

func (sb *ServerBootstrap) GenerateToken(validDuration time.Duration, multiUse bool, maxUses int) (*BootstrapToken, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("failed to generate token: %w", err)
	}

	token := hex.EncodeToString(tokenBytes)

	bt := &BootstrapToken{
		Token:      token,
		ExpiresAt:  time.Now().Add(validDuration),
		MultiUse:   multiUse,
		UsageCount: 0,
		MaxUses:    maxUses,
	}

	sb.tokens[token] = bt

	if err := sb.SaveState(); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	return bt, nil
}

func (sb *ServerBootstrap) allocateIP() (netip.Addr, error) {
	baseIP := sb.networkPrefix.Addr()
	bits := sb.networkPrefix.Bits()

	maxHosts := 1 << (32 - bits) // 2^(32-bits)

	for offset := 1; offset < maxHosts-1; offset++ { // Skip network and broadcast
		ipBytes := baseIP.As4()

		val := uint32(ipBytes[0])<<24 | uint32(ipBytes[1])<<16 | uint32(ipBytes[2])<<8 | uint32(ipBytes[3])
		val += uint32(offset)

		newIPBytes := [4]byte{
			byte(val >> 24),
			byte(val >> 16),
			byte(val >> 8),
			byte(val),
		}

		newIP := netip.AddrFrom4(newIPBytes)

		if !sb.networkPrefix.Contains(newIP) {
			continue
		}

		ipStr := newIP.String()
		if !sb.usedIPs[ipStr] {
			sb.usedIPs[ipStr] = true
			return newIP, nil
		}
	}

	return netip.Addr{}, fmt.Errorf("no available IPs in network")
}

// GenerateAgentConfig generates a new agent configuration without using a bootstrap token.
// This is useful for server-side agent provisioning or manual enrollment.
// The caller must hold sb.mu.Lock() before calling this function.
func (sb *ServerBootstrap) GenerateAgentConfig() (*AgentConfig, error) {
	// Generate agent keys
	privKey, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		return nil, fmt.Errorf("failed to generate agent key: %w", err)
	}

	pubKey := privKey.PublicKey()

	// Allocate IP
	assignedIP, err := sb.allocateIP()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate IP: %w", err)
	}

	// Generate unique agent ID
	agentID := uuid.New().String()

	config := &AgentConfig{
		AgentID:       agentID,
		PrivateKey:    hex.EncodeToString(privKey[:]),
		PublicKey:     hex.EncodeToString(pubKey[:]),
		AssignedIP:    assignedIP.String(),
		ServerPubKey:  sb.serverPublicKey,
		ServerAddress: sb.serverAddress,
		ServerPort:    sb.serverPort,
		AllowedIPs:    []string{sb.networkPrefix.String()},
		EnrolledAt:    time.Now(),
	}

	sb.agents[agentID] = config

	if err := sb.SaveState(); err != nil {
		return nil, fmt.Errorf("failed to save state: %w", err)
	}

	sb.notifyPeerChange()

	return config, nil
}

// CreateAgent creates a new agent configuration directly without using a bootstrap token.
// This is a convenience wrapper around GenerateAgentConfig that handles locking.
func (sb *ServerBootstrap) CreateAgent() (*AgentConfig, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	return sb.GenerateAgentConfig()
}

func (sb *ServerBootstrap) EnrollAgent(token string) (*AgentConfig, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Validate token
	bt, exists := sb.tokens[token]
	if !exists {
		return nil, fmt.Errorf("invalid token")
	}

	if time.Now().After(bt.ExpiresAt) {
		delete(sb.tokens, token)
		return nil, fmt.Errorf("token expired")
	}

	// Check if multi-use token has reached max uses
	if bt.MultiUse && bt.MaxUses > 0 && bt.UsageCount >= bt.MaxUses {
		delete(sb.tokens, token)
		return nil, fmt.Errorf("token usage limit reached")
	}

	// Generate the agent config
	config, err := sb.GenerateAgentConfig()
	if err != nil {
		return nil, err
	}

	// Update token usage
	if bt.MultiUse {
		bt.UsageCount++
		// Save state again to update token usage count
		if err := sb.SaveState(); err != nil {
			return nil, fmt.Errorf("failed to save token state: %w", err)
		}
	} else {
		delete(sb.tokens, token)
		// Save state again to remove single-use token
		if err := sb.SaveState(); err != nil {
			return nil, fmt.Errorf("failed to save token state: %w", err)
		}
	}

	return config, nil
}

func (sb *ServerBootstrap) GetAgentConfig(agentID string) (*AgentConfig, bool) {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	config, exists := sb.agents[agentID]
	return config, exists
}

func (sb *ServerBootstrap) ListAgents() []*AgentConfig {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	agents := make([]*AgentConfig, 0, len(sb.agents))
	for _, config := range sb.agents {
		agents = append(agents, config)
	}
	return agents
}

func (sb *ServerBootstrap) RemoveAgent(agentID string) error {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	config, exists := sb.agents[agentID]
	if !exists {
		return fmt.Errorf("agent not found: %s", agentID)
	}

	// Free the IP
	delete(sb.usedIPs, config.AssignedIP)
	delete(sb.agents, agentID)

	// Save state
	if err := sb.SaveState(); err != nil {
		return fmt.Errorf("failed to save state: %w", err)
	}

	sb.notifyPeerChange()

	return nil
}

func (sb *ServerBootstrap) GetUsedIPs() []string {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	ips := make([]string, 0, len(sb.usedIPs))
	for ip := range sb.usedIPs {
		ips = append(ips, ip)
	}
	return ips
}

func (sb *ServerBootstrap) GetAvailableIPCount() int {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	bits := sb.networkPrefix.Bits()
	maxHosts := (1 << (32 - bits)) - 2 // Exclude network and broadcast
	return maxHosts - len(sb.usedIPs)
}

func (sb *ServerBootstrap) buildPeerConfigs() []wireguard.PeerConfig {
	peers := make([]wireguard.PeerConfig, 0, len(sb.agents))
	for _, agent := range sb.agents {
		peers = append(peers, wireguard.PeerConfig{
			PublicKey: agent.PublicKey,
			AllowedIPs: []netip.Prefix{
				netip.MustParsePrefix(agent.AssignedIP + "/32"),
			},
		})
	}
	return peers
}

func (sb *ServerBootstrap) BuildServerNode(ctx context.Context, listenAddr string) (*arpc.Node, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	// Build peer configs for all enrolled agents
	peers := sb.buildPeerConfigs()

	// Create WireGuard transport
	transport, err := wireguard.New(ctx, wireguard.Config{
		PrivateKey: sb.serverPrivateKey,
		ListenPort: sb.serverPort,
		LocalIP:    sb.networkPrefix.Addr(),
		Peers:      peers,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create node
	node, err := arpc.NewNode(transport, listenAddr)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	sb.activeNode = node

	// Start peer change listener
	go sb.watchPeerChanges(ctx, transport)

	return node, nil
}

func (sb *ServerBootstrap) watchPeerChanges(ctx context.Context, transport *wireguard.Transport) {
	changeNotifier := make(chan struct{}, 10)

	sb.mu.Lock()
	sb.peerChangeListeners = append(sb.peerChangeListeners, changeNotifier)
	sb.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case <-changeNotifier:
			sb.mu.RLock()
			peers := sb.buildPeerConfigs()
			sb.mu.RUnlock()

			// Update transport with new peer list
			if err := transport.UpdatePeers(peers); err != nil {
				// Log error or handle appropriately
				fmt.Fprintf(os.Stderr, "Failed to update peers: %v\n", err)
			}
		}
	}
}

func (sb *ServerBootstrap) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Read token from request
	var req struct {
		Token string `json:"token"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	// Enroll agent
	config, err := sb.EnrollAgent(req.Token)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	// Return configuration
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(config)
}

type AgentBootstrap struct{}

func NewAgentBootstrap() *AgentBootstrap {
	return &AgentBootstrap{}
}

func (ab *AgentBootstrap) Enroll(ctx context.Context, serverURL, token string) (*AgentConfig, error) {
	reqBody, err := json.Marshal(map[string]string{
		"token": token,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, serverURL+"/enroll", bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrollment request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("enrollment failed with status: %d", resp.StatusCode)
	}

	var config AgentConfig
	if err := json.NewDecoder(resp.Body).Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	return &config, nil
}

func (ab *AgentBootstrap) BuildAgentNode(ctx context.Context, config *AgentConfig, listenAddr string) (*arpc.Node, error) {
	// Parse allowed IPs
	allowedIPs := make([]netip.Prefix, 0, len(config.AllowedIPs))
	for _, ipStr := range config.AllowedIPs {
		prefix, err := netip.ParsePrefix(ipStr)
		if err != nil {
			return nil, fmt.Errorf("invalid allowed IP %s: %w", ipStr, err)
		}
		allowedIPs = append(allowedIPs, prefix)
	}

	// Create WireGuard transport
	transport, err := wireguard.New(ctx, wireguard.Config{
		PrivateKey: config.PrivateKey,
		LocalIP:    netip.MustParseAddr(config.AssignedIP),
		Peers: []wireguard.PeerConfig{
			{
				PublicKey:  config.ServerPubKey,
				Endpoint:   fmt.Sprintf("%s:%d", config.ServerAddress, config.ServerPort),
				AllowedIPs: allowedIPs,
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	// Create node
	node, err := arpc.NewNode(transport, listenAddr)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	return node, nil
}

func (ab *AgentBootstrap) SaveConfig(config *AgentConfig, filepath string) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	return nil
}

func (ab *AgentBootstrap) ConfigToString(config *AgentConfig) (string, error) {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal config: %w", err)
	}

	return string(data), nil
}

func (ab *AgentBootstrap) LoadConfig(filepath string) (*AgentConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config AgentConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}
