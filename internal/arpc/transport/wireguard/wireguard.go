package wireguard

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"

	"golang.zx2c4.com/wireguard/conn"
	"golang.zx2c4.com/wireguard/device"
	"golang.zx2c4.com/wireguard/tun/netstack"
)

// Transport implements transport.Transport using WireGuard
type Transport struct {
	dev        *device.Device
	tnet       *netstack.Net
	localIP    netip.Addr
	privateKey string
	listenPort int

	mu     sync.RWMutex
	peers  []PeerConfig
	closed bool
}

// Config contains WireGuard-specific configuration
type Config struct {
	PrivateKey string
	ListenPort int
	Peers      []PeerConfig
	LocalIP    netip.Addr
	DNS        []netip.Addr
	MTU        int
}

type PeerConfig struct {
	PublicKey    string
	Endpoint     string
	AllowedIPs   []netip.Prefix
	PresharedKey string
}

// New creates a new WireGuard transport
func New(ctx context.Context, cfg Config) (*Transport, error) {
	if cfg.MTU == 0 {
		cfg.MTU = 1420
	}

	tunDev, tnet, err := netstack.CreateNetTUN(
		[]netip.Addr{cfg.LocalIP},
		cfg.DNS,
		cfg.MTU,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create TUN: %w", err)
	}

	logger := device.NewLogger(device.LogLevelError, "[wg] ")
	dev := device.NewDevice(tunDev, conn.NewDefaultBind(), logger)

	ipcConfig := buildIPCConfig(cfg)

	if err := dev.IpcSet(ipcConfig); err != nil {
		tunDev.Close()
		return nil, fmt.Errorf("failed to configure device: %w", err)
	}

	if err := dev.Up(); err != nil {
		tunDev.Close()
		return nil, fmt.Errorf("failed to bring up device: %w", err)
	}

	transport := &Transport{
		dev:        dev,
		tnet:       tnet,
		localIP:    cfg.LocalIP,
		privateKey: cfg.PrivateKey,
		listenPort: cfg.ListenPort,
		peers:      cfg.Peers,
	}

	go func() {
		<-ctx.Done()
		transport.Close()
	}()

	return transport, nil
}

func (t *Transport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, fmt.Errorf("transport is closed")
	}

	return t.tnet.DialContext(ctx, network, address)
}

func (t *Transport) Listen(network, address string) (net.Listener, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, fmt.Errorf("transport is closed")
	}

	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid address: %w", err)
	}

	if host == "" || host == "0.0.0.0" || host == "::" {
		host = t.localIP.String()
	}

	port, err := net.LookupPort(network, portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port: %w", err)
	}

	addr := netip.AddrPortFrom(netip.MustParseAddr(host), uint16(port))

	switch network {
	case "tcp", "tcp4", "tcp6":
		return t.tnet.ListenTCPAddrPort(addr)
	default:
		return nil, fmt.Errorf("unsupported network: %s", network)
	}
}

func (t *Transport) LocalAddr() netip.Addr {
	return t.localIP
}

// UpdatePeers updates the peer list dynamically without recreating the transport
func (t *Transport) UpdatePeers(peers []PeerConfig) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return fmt.Errorf("transport is closed")
	}

	// Build new IPC config with updated peers
	cfg := Config{
		PrivateKey: t.privateKey,
		ListenPort: t.listenPort,
		Peers:      peers,
		LocalIP:    t.localIP,
	}

	// First, remove all existing peers
	removeConfig := fmt.Sprintf("private_key=%s\n", t.privateKey)
	if t.listenPort > 0 {
		removeConfig += fmt.Sprintf("listen_port=%d\n", t.listenPort)
	}

	// Mark all existing peers for removal
	for _, peer := range t.peers {
		removeConfig += fmt.Sprintf("public_key=%s\nremove=true\n", peer.PublicKey)
	}

	if err := t.dev.IpcSet(removeConfig); err != nil {
		return fmt.Errorf("failed to remove old peers: %w", err)
	}

	// Then add the new peers
	ipcConfig := buildIPCConfig(cfg)
	if err := t.dev.IpcSet(ipcConfig); err != nil {
		return fmt.Errorf("failed to update peers: %w", err)
	}

	// Update stored peer list
	t.peers = peers

	return nil
}

// GetPeers returns the current peer configuration
func (t *Transport) GetPeers() []PeerConfig {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Return a copy to prevent external modification
	peers := make([]PeerConfig, len(t.peers))
	copy(peers, t.peers)
	return peers
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	t.dev.Close()
	return nil
}

func buildIPCConfig(cfg Config) string {
	ipc := fmt.Sprintf("private_key=%s\n", cfg.PrivateKey)

	if cfg.ListenPort > 0 {
		ipc += fmt.Sprintf("listen_port=%d\n", cfg.ListenPort)
	}

	for _, peer := range cfg.Peers {
		ipc += fmt.Sprintf("public_key=%s\n", peer.PublicKey)

		if peer.Endpoint != "" {
			ipc += fmt.Sprintf("endpoint=%s\n", peer.Endpoint)
		}

		if peer.PresharedKey != "" {
			ipc += fmt.Sprintf("preshared_key=%s\n", peer.PresharedKey)
		}

		for _, allowedIP := range peer.AllowedIPs {
			ipc += fmt.Sprintf("allowed_ip=%s\n", allowedIP.String())
		}

		ipc += "persistent_keepalive_interval=25\n"
	}

	return ipc
}
