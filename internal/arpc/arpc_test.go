package arpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	_ "net/http/pprof"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	binarystream "github.com/pbs-plus/pbs-plus/internal/arpc/binary"
	"github.com/pbs-plus/pbs-plus/internal/arpc/transport/wireguard"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

// generateKeypair generates a WireGuard keypair
func generateKeypair() (privateKey, publicKey string) {
	key, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		panic(err)
	}
	pubKey := key.PublicKey()
	privateKey = hex.EncodeToString(key[:])
	publicKey = hex.EncodeToString(pubKey[:])
	return
}

// ---------------------------------------------------------------------
// Helper: setupNodePair creates a pair of nodes with net.Pipe
// ---------------------------------------------------------------------
func setupNodePair(t *testing.T) (client *Node, server *Node, cleanup func()) {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	// Create mock transports that use the pipe connections
	clientTransport := &mockTransport{conn: clientConn}
	serverTransport := &mockTransport{conn: serverConn}

	ctx, cancel := context.WithCancel(context.Background())

	// Create nodes (they won't actually listen in this test setup)
	client = &Node{
		transport:  clientTransport,
		Router:     NewRouter(),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	server = &Node{
		transport:  serverTransport,
		Router:     NewRouter(),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	// Start serving on server side in a goroutine
	go func() {
		session, err := NewSession(serverConn)
		if err != nil {
			return
		}
		session.SetRouter(server.Router)
		session.Serve()
	}()

	// Give server time to start serving
	time.Sleep(10 * time.Millisecond)

	// Create client peer reference (no actual connection yet)
	clientPeer := &Peer{
		addr:      "test-server",
		transport: clientTransport,
		ctx:       ctx,
	}
	client.peers.Store("test-server", clientPeer)

	cleanup = func() {
		cancel()
		clientConn.Close()
		serverConn.Close()
	}

	return client, server, cleanup
}

// mockTransport is a simple transport for testing
type mockTransport struct {
	conn net.Conn
	mu   sync.Mutex
}

func (m *mockTransport) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn == nil {
		return nil, fmt.Errorf("mock connection is nil")
	}
	return m.conn, nil
}

func (m *mockTransport) Listen(network, address string) (net.Listener, error) {
	return nil, fmt.Errorf("mock transport does not support listening")
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		err := m.conn.Close()
		m.conn = nil
		return err
	}
	return nil
}

func (m *mockTransport) LocalAddr() netip.Addr {
	return netip.MustParseAddr("127.0.0.1")
}

// ---------------------------------------------------------------------
// Helper: setupWireGuardNodes
// Creates a pair of WireGuard-backed nodes for integration testing
// ---------------------------------------------------------------------
func setupWireGuardNodes(t *testing.T) (serverNode, clientNode *Node, cleanup func()) {
	t.Helper()

	ctx := context.Background()

	// Generate keypairs
	serverPriv, serverPub := generateKeypair()
	clientPriv, clientPub := generateKeypair()

	// Create server transport
	serverTransport, err := wireguard.New(ctx, wireguard.Config{
		PrivateKey: serverPriv,
		ListenPort: 51820,
		LocalIP:    netip.MustParseAddr("10.0.0.1"),
		MTU:        1420,
		Peers: []wireguard.PeerConfig{
			{
				PublicKey:  clientPub,
				Endpoint:   "127.0.0.1:51821",
				AllowedIPs: []netip.Prefix{netip.MustParsePrefix("10.0.0.2/32")},
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create server transport: %v", err)
	}

	// Create client transport
	clientTransport, err := wireguard.New(ctx, wireguard.Config{
		PrivateKey: clientPriv,
		ListenPort: 51821,
		LocalIP:    netip.MustParseAddr("10.0.0.2"),
		MTU:        1420,
		Peers: []wireguard.PeerConfig{
			{
				PublicKey:  serverPub,
				Endpoint:   "127.0.0.1:51820",
				AllowedIPs: []netip.Prefix{netip.MustParsePrefix("10.0.0.0/24")},
			},
		},
	})
	if err != nil {
		serverTransport.Close()
		t.Fatalf("failed to create client transport: %v", err)
	}

	// Give tunnels time to establish handshake
	time.Sleep(100 * time.Millisecond)

	// Create nodes
	serverNode, err = NewNode(serverTransport, "10.0.0.1:8080")
	if err != nil {
		clientTransport.Close()
		serverTransport.Close()
		t.Fatalf("failed to create server node: %v", err)
	}

	clientNode, err = NewNode(clientTransport, "10.0.0.2:8080")
	if err != nil {
		serverNode.Close()
		t.Fatalf("failed to create client node: %v", err)
	}

	// Start both nodes
	serverNode.Start()
	clientNode.Start()

	time.Sleep(50 * time.Millisecond)

	cleanup = func() {
		clientNode.Close()
		serverNode.Close()
	}

	return serverNode, clientNode, cleanup
}

// ---------------------------------------------------------------------
// Test 1: Router.ServeConn working as expected (Echo handler)
// ---------------------------------------------------------------------
func TestRouterServeConn_Echo(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	router := NewRouter()
	router.Handle("echo", func(req Request) (Response, error) {
		return Response{
			Status: 200,
			Data:   req.Payload,
		}, nil
	})

	var (
		wg     sync.WaitGroup
		srvErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		srvErr = router.ServeConn(serverConn)
	}()

	payload := StringMsg("hello")
	payloadBytes, err := payload.Encode()
	if err != nil {
		t.Fatalf("failed to encode payload: %v", err)
	}

	req := Request{
		Method:  "echo",
		Payload: payloadBytes,
	}

	reqBytes, err := req.Encode()
	if err != nil {
		t.Fatalf("failed to encode request: %v", err)
	}

	if _, err := clientConn.Write(reqBytes); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	respBuf := make([]byte, 1024)
	n, err := clientConn.Read(respBuf)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var resp Response
	if err := resp.Decode(respBuf[:n]); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	var echoed StringMsg
	if err := echoed.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode echoed data: %v", err)
	}
	if echoed != "hello" {
		t.Fatalf("expected data 'hello', got %q", echoed)
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for ServeConn to finish")
	}

	if srvErr != nil {
		t.Logf("server finished with: %v", srvErr)
	}
}

// ---------------------------------------------------------------------
// Test 2: Node Peer Call (simple call)
// ---------------------------------------------------------------------
func TestNodePeerCall_Success(t *testing.T) {
	client, server, cleanup := setupNodePair(t)
	defer cleanup()

	server.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	peer, err := client.GetPeer("test-server")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	resp, err := peer.Call("ping", nil)
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if resp.Status != 200 {
		t.Fatalf("expected status 200, got %d", resp.Status)
	}

	var pong StringMsg
	if err := pong.Decode(resp.Data); err != nil {
		t.Fatalf("failed to decode pong: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected pong response, got %q", pong)
	}
}

// ---------------------------------------------------------------------
// Test 3: WireGuard Node - Basic connectivity
// ---------------------------------------------------------------------
func TestWireGuardNode_BasicConnectivity(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	// Register handler on server
	serverNode.Handle("ping", func(req Request) (Response, error) {
		var pong StringMsg = "pong"
		pongBytes, _ := pong.Encode()
		return Response{
			Status: 200,
			Data:   pongBytes,
		}, nil
	})

	// Client connects to server
	peer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	// Make RPC call
	data, err := peer.CallMsg(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("CallMsg failed: %v", err)
	}

	var pong StringMsg
	if err := pong.Decode(data); err != nil {
		t.Fatalf("failed to decode pong: %v", err)
	}
	if pong != "pong" {
		t.Fatalf("expected pong response, got %q", pong)
	}
}

// ---------------------------------------------------------------------
// Test 4: WireGuard Node - Bidirectional communication
// ---------------------------------------------------------------------
func TestWireGuardNode_Bidirectional(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	// Server can handle requests from client
	serverNode.Handle("server_ping", func(req Request) (Response, error) {
		var msg StringMsg = "server_pong"
		msgBytes, _ := msg.Encode()
		return Response{Status: 200, Data: msgBytes}, nil
	})

	// Client can handle requests from server
	clientNode.Handle("client_ping", func(req Request) (Response, error) {
		var msg StringMsg = "client_pong"
		msgBytes, _ := msg.Encode()
		return Response{Status: 200, Data: msgBytes}, nil
	})

	// Client calls server
	serverPeer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get server peer: %v", err)
	}

	data, err := serverPeer.CallMsg(context.Background(), "server_ping", nil)
	if err != nil {
		t.Fatalf("client->server call failed: %v", err)
	}

	var serverResp StringMsg
	if err := serverResp.Decode(data); err != nil {
		t.Fatalf("failed to decode server response: %v", err)
	}
	if serverResp != "server_pong" {
		t.Fatalf("expected server_pong, got %q", serverResp)
	}

	// Server calls client
	clientPeer, err := serverNode.GetPeer("10.0.0.2:8080")
	if err != nil {
		t.Fatalf("failed to get client peer: %v", err)
	}

	data, err = clientPeer.CallMsg(context.Background(), "client_ping", nil)
	if err != nil {
		t.Fatalf("server->client call failed: %v", err)
	}

	var clientResp StringMsg
	if err := clientResp.Decode(data); err != nil {
		t.Fatalf("failed to decode client response: %v", err)
	}
	if clientResp != "client_pong" {
		t.Fatalf("expected client_pong, got %q", clientResp)
	}
}

// ---------------------------------------------------------------------
// Test 5: WireGuard Node - Multiple concurrent connections
// ---------------------------------------------------------------------
func TestWireGuardNode_Concurrency(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	serverNode.Handle("echo", func(req Request) (Response, error) {
		return Response{
			Status: 200,
			Data:   req.Payload,
		}, nil
	})

	// Get peer reference once
	peer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	// Launch multiple concurrent requests using the same peer
	const numRequests = 20
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			req := StringMsg(fmt.Sprintf("message %d", id))
			data, err := peer.CallMsg(context.Background(), "echo", &req)
			if err != nil {
				t.Errorf("Request %d: CallMsg failed: %v", id, err)
				return
			}

			var resp StringMsg
			if err := resp.Decode(data); err != nil {
				t.Errorf("Request %d: failed to decode response: %v", id, err)
				return
			}

			expected := fmt.Sprintf("message %d", id)
			if string(resp) != expected {
				t.Errorf("Request %d: expected %q, got %q", id, expected, resp)
			}
		}(i)
	}

	wg.Wait()
}

// ---------------------------------------------------------------------
// Test 6: WireGuard Node - CallBinary
// ---------------------------------------------------------------------
func TestWireGuardNode_CallBinary(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	serverNode.Handle("download", func(req Request) (Response, error) {
		data := []byte("this is binary data from the server")

		return Response{
			Status: 213,
			RawStream: func(conn net.Conn) {
				r := bytes.NewReader(data)
				if err := binarystream.SendDataFromReader(r, len(data), conn); err != nil {
					t.Logf("error sending binary data: %v", err)
				}
			},
		}, nil
	})

	peer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	buffer := make([]byte, 1024)
	n, err := peer.CallBinary(context.Background(), "download", nil, buffer)
	if err != nil {
		t.Fatalf("CallBinary failed: %v", err)
	}

	expected := "this is binary data from the server"
	if n != len(expected) {
		t.Fatalf("expected %d bytes, got %d", len(expected), n)
	}

	if got := string(buffer[:n]); got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

// ---------------------------------------------------------------------
// Test 7: CallContext with timeout
// ---------------------------------------------------------------------
func TestCallContext_Timeout(t *testing.T) {
	client, server, cleanup := setupNodePair(t)
	defer cleanup()

	server.Handle("slow", func(req Request) (Response, error) {
		time.Sleep(200 * time.Millisecond)
		var done StringMsg = "done"
		doneBytes, _ := done.Encode()
		return Response{
			Status: 200,
			Data:   doneBytes,
		}, nil
	})

	peer, err := client.GetPeer("test-server")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = peer.CallMsg(ctx, "slow", nil)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// ---------------------------------------------------------------------
// Test 8: CallBinary_Success (net.Pipe version)
// ---------------------------------------------------------------------
func TestCallBinary_Success(t *testing.T) {
	client, server, cleanup := setupNodePair(t)
	defer cleanup()

	server.Handle("buffer", func(req Request) (Response, error) {
		binaryData := []byte("hello world")

		return Response{
			Status: 213,
			RawStream: func(conn net.Conn) {
				r := bytes.NewReader(binaryData)
				if err := binarystream.SendDataFromReader(r, len(binaryData), conn); err != nil {
					t.Errorf("server: error sending binary data: %v", err)
				}
			},
		}, nil
	})

	peer, err := client.GetPeer("test-server")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	buffer := make([]byte, 1024)
	n, err := peer.CallBinary(context.Background(), "buffer", nil, buffer)
	if err != nil {
		t.Fatalf("client: CallBinary error: %v", err)
	}

	expected := "hello world"
	if n != len(expected) {
		t.Fatalf("expected %d bytes, got %d", len(expected), n)
	}
	got := string(buffer[:n])
	if got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

// ---------------------------------------------------------------------
// Test 9: CallMsg_ErrorResponse
// ---------------------------------------------------------------------
func TestCallMsg_ErrorResponse(t *testing.T) {
	client, server, cleanup := setupNodePair(t)
	defer cleanup()

	server.Handle("error", func(req Request) (Response, error) {
		return Response{}, errors.New("test error")
	})

	peer, err := client.GetPeer("test-server")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	data, err := peer.CallMsg(context.Background(), "error", nil)
	if err == nil {
		t.Fatal("expected error response from CallMsg, got nil")
	}
	if !strings.Contains(err.Error(), "test error") {
		t.Fatalf("expected error to contain 'test error', got: %v", err)
	}
	if data != nil {
		t.Fatalf("expected no returned data on error response, got: %v", data)
	}
}

// ---------------------------------------------------------------------
// Test 10: CallBinary_ErrorResponse
// ---------------------------------------------------------------------
func TestCallBinary_ErrorResponse(t *testing.T) {
	client, server, cleanup := setupNodePair(t)
	defer cleanup()

	server.Handle("buffer_error", func(req Request) (Response, error) {
		return Response{}, errors.New("buffer error occurred")
	})

	peer, err := client.GetPeer("test-server")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	buffer := make([]byte, 1024)
	n, err := peer.CallBinary(context.Background(), "buffer_error", nil, buffer)
	if err == nil {
		t.Fatal("expected error response from CallBinary, got nil")
	}
	if !strings.Contains(err.Error(), "buffer error occurred") {
		t.Fatalf("expected error message to contain 'buffer error occurred', got: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes read on error response, got %d", n)
	}
}

// ---------------------------------------------------------------------
// Test 11: CallBinary_Concurrency
// ---------------------------------------------------------------------
func TestCallBinary_Concurrency(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	serverNode.Handle("binary_concurrent", func(req Request) (Response, error) {
		var payload MapStringIntMsg
		clientID := 0
		if req.Payload != nil {
			if err := payload.Decode(req.Payload); err == nil {
				if v, ok := payload["id"]; ok {
					clientID = v
				}
			}
		}

		dataStr := fmt.Sprintf("binary data for client %d", clientID)
		binaryData := []byte(dataStr)

		return Response{
			Status: 213,
			RawStream: func(conn net.Conn) {
				r := bytes.NewReader(binaryData)
				if err := binarystream.SendDataFromReader(r, len(binaryData), conn); err != nil {
					t.Logf("server: error sending binary data for client %d: %v", clientID, err)
				}
			},
		}, nil
	})

	// Get peer reference once
	peer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	const numClients = 50
	var wg sync.WaitGroup

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			payload := MapStringIntMsg{"id": id}

			expectedSize := len(fmt.Sprintf("binary data for client %d", id))
			buffer := make([]byte, expectedSize+100)
			n, err := peer.CallBinary(context.Background(), "binary_concurrent", &payload, buffer)
			if err != nil {
				t.Errorf("client %d: CallBinary error: %v", id, err)
				return
			}

			expected := fmt.Sprintf("binary data for client %d", id)
			if n != len(expected) {
				t.Errorf("client %d: expected %d bytes, got %d", id, len(expected), n)
				return
			}
			if got := string(buffer[:n]); got != expected {
				t.Errorf("client %d: expected %q, got %q", id, expected, got)
			}
		}(i)
	}
	wg.Wait()
}

// ---------------------------------------------------------------------
// Test 12: Node.ListPeers
// ---------------------------------------------------------------------
func TestNode_ListPeers(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	serverNode.Handle("ping", func(req Request) (Response, error) {
		return Response{Status: 200}, nil
	})

	// Initially no peers
	if len(clientNode.ListPeers()) != 0 {
		t.Fatal("expected no peers initially")
	}

	// Connect to server
	_, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	// Should now have 1 peer
	peers := clientNode.ListPeers()
	if len(peers) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(peers))
	}
	if peers[0].Addr() != "10.0.0.1:8080" {
		t.Fatalf("expected peer address 10.0.0.1:8080, got %s", peers[0].Addr())
	}
}

// ---------------------------------------------------------------------
// Test 13: Node.RemovePeer
// ---------------------------------------------------------------------
func TestNode_RemovePeer(t *testing.T) {
	serverNode, clientNode, cleanup := setupWireGuardNodes(t)
	defer cleanup()

	serverNode.Handle("ping", func(req Request) (Response, error) {
		return Response{Status: 200}, nil
	})

	// Connect to server
	peer, err := clientNode.GetPeer("10.0.0.1:8080")
	if err != nil {
		t.Fatalf("failed to get peer: %v", err)
	}

	// Verify connection works
	_, err = peer.CallMsg(context.Background(), "ping", nil)
	if err != nil {
		t.Fatalf("initial call failed: %v", err)
	}

	// Remove peer
	if err := clientNode.RemovePeer("10.0.0.1:8080"); err != nil {
		t.Fatalf("failed to remove peer: %v", err)
	}

	// Verify peer list is empty
	if len(clientNode.ListPeers()) != 0 {
		t.Fatal("expected no peers after removal")
	}

	// Verify peer is closed
	if !peer.IsClosed() {
		t.Fatal("expected peer to be closed")
	}
}

// ---------------------------------------------------------------------
// BenchmarkNodePeerCall
// ---------------------------------------------------------------------
func BenchmarkNodePeerCall(b *testing.B) {
	const numClients = 10

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		wg.Add(numClients)

		for clientID := 0; clientID < numClients; clientID++ {
			go func(clientID int) {
				defer wg.Done()

				client, server, cleanup := setupNodePair(&testing.T{})
				defer cleanup()

				server.Handle("ping", func(req Request) (Response, error) {
					var pong StringMsg = "pong"
					pongBytes, _ := pong.Encode()
					return Response{
						Status: 200,
						Data:   pongBytes,
					}, nil
				})

				peer, err := client.GetPeer("test-server")
				if err != nil {
					b.Errorf("Client %d: failed to get peer: %v", clientID, err)
					return
				}

				resp, err := peer.Call("ping", nil)
				if err != nil {
					b.Errorf("Client %d: Call failed: %v", clientID, err)
					return
				}
				if resp.Status != 200 {
					b.Errorf("Client %d: Expected status 200, got %d", clientID, resp.Status)
				}
			}(clientID)
		}

		wg.Wait()
	}
	b.StopTimer()
}
