package auth

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func generateTestCerts(tempDir string) error {
	// Generate CA private key
	caPrivKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	// Generate CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "Test CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &caPrivKey.PublicKey, caPrivKey)
	if err != nil {
		return err
	}

	// Save CA certificate and private key
	caCertPath := filepath.Join(tempDir, "ca.crt")
	caKeyPath := filepath.Join(tempDir, "ca.key")

	certOut, err := os.Create(caCertPath)
	if err != nil {
		return err
	}
	pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	certOut.Close()

	keyOut, err := os.Create(caKeyPath)
	if err != nil {
		return err
	}
	pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caPrivKey)})
	keyOut.Close()

	// Generate server and agent certificates
	for _, name := range []string{"server", "agent"} {
		privKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			return err
		}

		template := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject: pkix.Name{
				CommonName: name,
			},
			NotBefore:   time.Now(),
			NotAfter:    time.Now().AddDate(1, 0, 0),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
			DNSNames:    []string{"localhost"},
		}

		certBytes, err := x509.CreateCertificate(rand.Reader, template, ca, &privKey.PublicKey, caPrivKey)
		if err != nil {
			return err
		}

		// Save certificate and private key
		certPath := filepath.Join(tempDir, name+".crt")
		keyPath := filepath.Join(tempDir, name+".key")

		certOut, err := os.Create(certPath)
		if err != nil {
			return err
		}
		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
		certOut.Close()

		keyOut, err := os.Create(keyPath)
		if err != nil {
			return err
		}
		pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)})
		keyOut.Close()
	}

	return nil
}

func setupTestServer(t *testing.T, tempDir string) (*Server, *httptest.Server) {
	server := NewServer()

	// Load test certificates
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(tempDir, "server.crt"),
		filepath.Join(tempDir, "server.key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap", server.handleBootstrap)
	mux.HandleFunc("/secure", server.handleSecureEndpoint)

	testServer := httptest.NewUnstartedServer(mux)
	testServer.TLS = tlsConfig
	testServer.StartTLS()

	return server, testServer
}

func setupTestClient(t *testing.T, tempDir string) *http.Client {
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(tempDir, "agent.crt"),
		filepath.Join(tempDir, "agent.key"),
	)
	if err != nil {
		t.Fatal(err)
	}

	caCert, err := os.ReadFile(filepath.Join(tempDir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}
}

func TestBootstrap(t *testing.T) {
	// Create temp directory for test certificates
	tempDir, err := os.MkdirTemp("", "mtls-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Generate test certificates
	if err := generateTestCerts(tempDir); err != nil {
		t.Fatal(err)
	}

	server, testServer := setupTestServer(t, tempDir)
	defer testServer.Close()

	client := setupTestClient(t, tempDir)

	// Test bootstrap
	req := AgentRequest{
		AgentID: "test-agent",
	}
	reqBody, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Post(testServer.URL+"/bootstrap", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	var response AgentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}

	if response.Token == "" {
		t.Error("Expected token in response")
	}

	// Verify token is stored in server
	server.tokenMutex.RLock()
	if !server.tokens[response.Token] {
		t.Error("Token not stored in server")
	}
	server.tokenMutex.RUnlock()
}

func TestSecureEndpoint(t *testing.T) {
	// Create temp directory for test certificates
	tempDir, err := os.MkdirTemp("", "mtls-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Generate test certificates
	if err := generateTestCerts(tempDir); err != nil {
		t.Fatal(err)
	}

	server, testServer := setupTestServer(t, tempDir)
	defer testServer.Close()

	client := setupTestClient(t, tempDir)

	// First bootstrap to get token
	bootstrapReq := AgentRequest{
		AgentID: "test-agent",
	}
	reqBody, _ := json.Marshal(bootstrapReq)
	resp, err := client.Post(testServer.URL+"/bootstrap", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	var bootstrapResp AgentResponse
	if err := json.NewDecoder(resp.Body).Decode(&bootstrapResp); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Verify token is stored in server
	server.tokenMutex.RLock()
	if !server.tokens[bootstrapResp.Token] {
		t.Error("Token not stored in server")
	}
	server.tokenMutex.RUnlock()

	// Test secure endpoint with token
	req := AgentRequest{
		AgentID: "test-agent",
		Data:    "test data",
	}
	reqBody, _ = json.Marshal(req)
	request, _ := http.NewRequest("POST", testServer.URL+"/secure", bytes.NewBuffer(reqBody))
	request.Header.Set("Authorization", bootstrapResp.Token)
	request.Header.Set("Content-Type", "application/json")

	resp, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status OK, got %v", resp.Status)
	}

	var response AgentResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}

	expectedMessage := "Received data: test data"
	if response.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, response.Message)
	}
}

func TestInvalidToken(t *testing.T) {
	// Create temp directory for test certificates
	tempDir, err := os.MkdirTemp("", "mtls-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Generate test certificates
	if err := generateTestCerts(tempDir); err != nil {
		t.Fatal(err)
	}

	_, testServer := setupTestServer(t, tempDir)
	defer testServer.Close()

	client := setupTestClient(t, tempDir)

	// Test secure endpoint with invalid token
	req := AgentRequest{
		AgentID: "test-agent",
		Data:    "test data",
	}
	reqBody, _ := json.Marshal(req)
	request, _ := http.NewRequest("POST", testServer.URL+"/secure", bytes.NewBuffer(reqBody))
	request.Header.Set("Authorization", "invalid-token")
	request.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected status Unauthorized, got %v", resp.Status)
	}
}
