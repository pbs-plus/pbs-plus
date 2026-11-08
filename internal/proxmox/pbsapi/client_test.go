package pbsapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func selfSignedPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "other"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestHostSnapshotsForwardsAuthAndPinsCertificate(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Cookie") != "PBSAuthCookie=ticket" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"authentication failed"}`))
			return
		}
		if r.URL.Path != "/api2/json/admin/datastore/ds/snapshots" || r.URL.Query().Get("ns") != "prod" || r.URL.Query().Get("backup-type") != "host" {
			t.Errorf("unexpected request %s", r.URL)
		}
		_, _ = w.Write([]byte(`{"data":[{"backup-type":"host","backup-id":"web","backup-time":5,"files":[{"filename":"web.pxar.didx"}],"verification":{"state":"ok"}}]}`))
	}))
	defer server.Close()

	certFile := filepath.Join(t.TempDir(), "proxy.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(certFile, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	client := New(server.URL, pinnedTLSConfig(certFile))

	auth, _ := http.NewRequest(http.MethodGet, "/", nil)
	auth.Header.Set("Cookie", "PBSAuthCookie=ticket")
	snaps, err := client.HostSnapshots(context.Background(), auth, "ds", "prod")
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].BackupID != "web" || snaps[0].Verification.State != "ok" || snaps[0].Files[0].Filename != "web.pxar.didx" {
		t.Fatalf("snapshots = %#v", snaps)
	}

	anonymous, _ := http.NewRequest(http.MethodGet, "/", nil)
	_, err = client.HostSnapshots(context.Background(), anonymous, "ds", "")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusUnauthorized || apiErr.Message != "authentication failed" {
		t.Fatalf("err = %v", err)
	}

	otherCert := filepath.Join(t.TempDir(), "other.pem")
	if err := os.WriteFile(otherCert, selfSignedPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = New(server.URL, pinnedTLSConfig(otherCert)).HostSnapshots(context.Background(), auth, "ds", "")
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected a certificate pin mismatch, got %v", err)
	}
}
