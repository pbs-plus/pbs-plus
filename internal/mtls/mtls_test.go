package mtls

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.com/go-extension/http"
	"gitlab.com/go-extension/tls"
)

type bootstrapResp struct {
	CertPEM string `json:"cert_pem"`
}
type tokenResp struct {
	Token string `json:"token"`
}

func makeBootstrapHandler(caCertFile, caKeyFile string, clientValidDays int) (http.HandlerFunc, error) {
	caCertPEM, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("read ca cert: %w", err)
	}
	caKeyPEM, err := os.ReadFile(caKeyFile)
	if err != nil {
		return nil, fmt.Errorf("read ca key: %w", err)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		csrPEM, err := ioReadAllLimit(r.Body, 1<<20) // 1MB limit
		if err != nil {
			http.Error(w, "read body error", http.StatusBadRequest)
			return
		}
		certPEM, err := SignCSR(caCertPEM, caKeyPEM, csrPEM, clientValidDays, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
		if err != nil {
			http.Error(w, "sign csr error: "+err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/x-pem-file")
		_, _ = w.Write(certPEM)
	}, nil
}

func startMTLSServer(addr, serverCertFile, serverKeyFile, caFile string, clientAuth tls.ClientAuthType, handler http.Handler) (*http.Server, error) {
	tlsCfg, err := BuildServerTLS(serverCertFile, serverKeyFile, caFile, "", nil, clientAuth, true)
	if err != nil {
		return nil, err
	}
	s := &http.Server{
		Addr:      addr,
		Handler:   handler,
		TLSConfig: tlsCfg,
	}
	go func() { _ = s.ListenAndServeTLS(serverCertFile, serverKeyFile) }()
	return s, nil
}

func shutdownServer(ctx context.Context, s *http.Server) error {
	return s.Shutdown(ctx)
}

func ioReadAllLimit(r io.Reader, limit int64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, r, limit+1); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if int64(buf.Len()) > limit {
		return nil, errors.New("body too large")
	}
	return buf.Bytes(), nil
}

func TestEndToEndMTLSWithCSRBootstrap(t *testing.T) {
	t.Parallel()

	outDir := t.TempDir()
	serverCert, serverKey, caCert, caKey, err := EnsureLocalCAAndServerCert(
		outDir,
		"Test Org",
		"Test CA",
		2048,
		2,
	)
	if err != nil {
		t.Fatalf("GenerateLocalCAAndServerCert: %v", err)
	}

	bootstrapHandler, err := makeBootstrapHandler(caCert, caKey, 1)
	if err != nil {
		t.Fatalf("makeBootstrapHandler: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/bootstrap", func(w http.ResponseWriter, r *http.Request) {
		rec := &captureWriter{hdr: http.Header{}}
		bootstrapHandler(rec, r)
		for k, v := range rec.hdr {
			w.Header()[k] = v
		}
		if rec.status != 0 {
			w.WriteHeader(rec.status)
		}
		if rec.status != 0 && rec.status != http.StatusOK {
			_, _ = w.Write(rec.buf.Bytes())
			return
		}
		resp := bootstrapResp{CertPEM: rec.buf.String()}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client cert required", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tokenResp{Token: "test-token"})
	})

	mux.HandleFunc("/secure", func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			http.Error(w, "client cert required", http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get("Authorization"); got != "test-token" {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("secure OK"))
	})

	addr := "127.0.0.1:45443"
	srv, err := startMTLSServer(
		addr,
		serverCert,
		serverKey,
		caCert,
		tls.RequireAndVerifyClientCert,
		mux,
	)
	if err != nil {
		t.Fatalf("startMTLSServer: %v", err)
	}
	t.Cleanup(func() {
		_ = shutdownServer(context.Background(), srv)
	})

	if err := waitForServer(addr, mustReadFile(t, caCert), 5*time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	csrPEM, keyPEM, err := GenerateCSR("test-client", 2048)
	if err != nil {
		t.Fatalf("GenerateCSR: %v", err)
	}

	caCertPEM := mustReadFile(t, caCert)

	signedClientCertPEM, err := SignCSR(
		caCertPEM,
		mustReadFile(t, caKey),
		csrPEM,
		1,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	)
	if err != nil {
		t.Fatalf("SignCSR: %v", err)
	}

	clientDir := t.TempDir()
	clientCertFile := filepath.Join(clientDir, "client.crt")
	clientKeyFile := filepath.Join(clientDir, "client.key")
	if err := os.WriteFile(clientCertFile, signedClientCertPEM, 0o644); err != nil {
		t.Fatalf("write client cert: %v", err)
	}
	if err := os.WriteFile(clientKeyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}

	clientTLS, err := BuildClientTLS(signedClientCertPEM, keyPEM, caCertPEM, nil)
	if err != nil {
		t.Fatalf("BuildClientTLS: %v", err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:     clientTLS,
			ForceAttemptHTTP2:   true,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Timeout: 5 * time.Second,
	}

	var tok tokenResp
	{
		resp, err := client.Get("https://" + addr + "/token")
		if err != nil {
			t.Fatalf("GET /token: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("token status: %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
			t.Fatalf("decode token: %v", err)
		}
		if tok.Token != "test-token" {
			t.Fatalf("unexpected token: %q", tok.Token)
		}
	}

	t.Run("ParallelSecureRequests", func(t *testing.T) {
		t.Parallel()
		var wg sync.WaitGroup
		errs := make(chan error, 6)

		doReq := func() {
			defer wg.Done()
			req, _ := http.NewRequest(
				"POST",
				"https://"+addr+"/secure",
				bytes.NewReader([]byte(`{"data":"msg"}`)),
			)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", tok.Token)
			resp, err := client.Do(req)
			if err != nil {
				errs <- err
				return
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("unexpected status: %d", resp.StatusCode)
			}
		}

		for i := 0; i < 6; i++ {
			wg.Add(1)
			go doReq()
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				t.Error(e)
			}
		}
	})

	t.Run("InvalidAuthorizationHeader", func(t *testing.T) {
		t.Parallel()
		req, _ := http.NewRequest(
			"POST",
			"https://"+addr+"/secure",
			bytes.NewReader([]byte(`{}`)),
		)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "invalid-token")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request err: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", resp.StatusCode)
		}
	})

	t.Run("MissingClientCert", func(t *testing.T) {
		t.Parallel()
		caPEM := mustReadFile(t, caCert)
		rootCAs := x509.NewCertPool()
		if !rootCAs.AppendCertsFromPEM(caPEM) {
			t.Fatalf("append CA failed")
		}
		noCertClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					KernelTX:   true,
					MinVersion: tls.VersionTLS12,
					RootCAs:    rootCAs,
					// no client certs
				},
				ForceAttemptHTTP2: true,
			},
			Timeout: 3 * time.Second,
		}
		_, err := noCertClient.Get("https://" + addr + "/secure")
		if err == nil {
			t.Fatalf("expected handshake failure without client cert")
		}
	})

	t.Run("TLSProperties", func(t *testing.T) {
		t.Parallel()
		req, _ := http.NewRequest("GET", "https://"+addr+"/secure", nil)
		req.Header.Set("Authorization", tok.Token)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request err: %v", err)
		}
		defer resp.Body.Close()
		cs := resp.TLS
		if cs == nil {
			t.Fatalf("expected TLS connection state")
		}
		if cs.Version < tls.VersionTLS12 {
			t.Fatalf("expected TLS version >= 1.2, got %x", cs.Version)
		}
		if len(cs.PeerCertificates) == 0 {
			t.Fatalf("expected server peer certificate")
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("unexpected status: %d", resp.StatusCode)
		}
	})
}

type captureWriter struct {
	hdr    http.Header
	status int
	buf    bytes.Buffer
}

func (c *captureWriter) Header() http.Header  { return c.hdr }
func (c *captureWriter) WriteHeader(code int) { c.status = code }
func (c *captureWriter) Write(b []byte) (int, error) {
	return c.buf.Write(b)
}

func waitForServer(hostport string, caPEM []byte, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("append CA failed")
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			KernelTX:   true,
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
		},
		TLSHandshakeTimeout: 1 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	cl := &http.Client{
		Transport: tr,
		Timeout:   1500 * time.Millisecond,
	}
	url := "https://" + hostport + "/secure"
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for server at %s", hostport)
		}
		resp, err := cl.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		es := err.Error()
		if strings.Contains(es, "remote error: tls:") ||
			strings.Contains(es, "handshake failure") ||
			strings.Contains(es, "certificate required") ||
			strings.Contains(es, "bad certificate") {
			return nil
		}
		if strings.Contains(es, "connection refused") {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
