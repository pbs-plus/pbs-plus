package arpc

import (
	"context"
	"fmt"
	"net"
	"sync"
)

type Transport interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	Listen(network, address string) (net.Listener, error)
	Close() error
}

type Node struct {
	transport Transport
	Listener  net.Listener
	Router    Router

	peers    sync.Map // map[string]*Peer - peer references by address
	handlers sync.Map // map[string]HandlerFunc - method handlers

	ctx        context.Context
	cancelFunc context.CancelFunc
}

func NewNode(transport Transport, listenAddr string) (*Node, error) {
	lis, err := transport.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	node := &Node{
		transport:  transport,
		Listener:   lis,
		Router:     NewRouter(),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	return node, nil
}

func (n *Node) AdditionalListener(listenAddr string) (net.Listener, error) {
	lis, err := n.transport.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	return lis, nil
}

func (n *Node) Handle(method string, handler HandlerFunc) {
	n.Router.Handle(method, handler)
}

func (n *Node) Start() error {
	go n.acceptLoop()
	return nil
}

func (n *Node) acceptLoop() {
	for {
		select {
		case <-n.ctx.Done():
			return
		default:
		}

		conn, err := n.Listener.Accept()
		if err != nil {
			if n.ctx.Err() != nil {
				return
			}
			continue
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	session, err := NewSession(conn)
	if err != nil {
		conn.Close()
		return
	}

	session.SetRouter(n.Router)
	session.Serve()
}

// GetPeer returns a peer reference (not a persistent connection)
func (n *Node) GetPeer(addr string) (*Peer, error) {
	// Check if we already have a peer reference for this address
	if p, ok := n.peers.Load(addr); ok {
		peer := p.(*Peer)
		if !peer.IsClosed() {
			return peer, nil
		}
		n.peers.Delete(addr)
	}

	// Create a new peer reference (no connection yet)
	peer := &Peer{
		addr:      addr,
		transport: n.transport,
		ctx:       n.ctx,
	}

	n.peers.Store(addr, peer)
	return peer, nil
}

func (n *Node) ListPeers() []*Peer {
	var peers []*Peer
	n.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if !peer.IsClosed() {
			peers = append(peers, peer)
		}
		return true
	})
	return peers
}

func (n *Node) RemovePeer(addr string) error {
	if p, ok := n.peers.LoadAndDelete(addr); ok {
		return p.(*Peer).Close()
	}
	return nil
}

func (n *Node) Close() error {
	n.cancelFunc()

	n.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		peer.Close()
		return true
	})

	if err := n.Listener.Close(); err != nil {
		return err
	}

	return n.transport.Close()
}
