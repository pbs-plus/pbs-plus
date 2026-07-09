package registry

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

func generateTestCertPEM(t *testing.T) (certPEM string, keyPEM string, certDER []byte, keyDER []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	certDERBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	keyDERBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDERBytes}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDERBytes}))

	return certPEM, keyPEM, certDERBytes, keyDERBytes
}

func TestUnwrapBase64Layers_PlainPEM(t *testing.T) {
	certPEM, keyPEM, _, _ := generateTestCertPEM(t)

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"cert PEM directly", "Cert", certPEM},
		{"priv PEM directly", "Priv", keyPEM},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unwrapBase64Layers(tt.val, tt.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tt.val {
				t.Errorf("expected unchanged PEM, got different output")
			}
		})
	}
}

func TestUnwrapBase64Layers_Base64PEM(t *testing.T) {
	certPEM, keyPEM, _, _ := generateTestCertPEM(t)

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"cert base64(PEM)", "Cert", base64.StdEncoding.EncodeToString([]byte(certPEM))},
		{"priv base64(PEM)", "Priv", base64.StdEncoding.EncodeToString([]byte(keyPEM))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unwrapBase64Layers(tt.val, tt.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isPEMData(result) {
				t.Errorf("expected PEM output, got: %q", result[:80])
			}
			_, _ = pem.Decode([]byte(result))
		})
	}
}

func TestUnwrapBase64Layers_Base64DER(t *testing.T) {
	_, _, certDER, keyDER := generateTestCertPEM(t)

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"cert base64(DER)", "Cert", base64.StdEncoding.EncodeToString(certDER)},
		{"priv base64(DER)", "Priv", base64.StdEncoding.EncodeToString(keyDER)},
		{"serverca base64(DER)", "ServerCA", base64.StdEncoding.EncodeToString(certDER)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unwrapBase64Layers(tt.val, tt.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isPEMData(result) {
				t.Errorf("expected PEM output, got: %q", result[:80])
			}
			block, _ := pem.Decode([]byte(result))
			if block == nil {
				t.Fatal("failed to decode PEM block")
			}
			if tt.key == "Priv" {
				_, err = x509.ParsePKCS8PrivateKey(block.Bytes)
				if err != nil {
					t.Errorf("failed to parse private key from wrapped DER: %v", err)
				}
			} else {
				_, err = x509.ParseCertificate(block.Bytes)
				if err != nil {
					t.Errorf("failed to parse certificate from wrapped DER: %v", err)
				}
			}
		})
	}
}

func TestUnwrapBase64Layers_DoubleBase64DER(t *testing.T) {
	_, _, certDER, keyDER := generateTestCertPEM(t)

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"cert base64(base64(DER))", "Cert", base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString(certDER)))},
		{"priv base64(base64(DER))", "Priv", base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString(keyDER)))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unwrapBase64Layers(tt.val, tt.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isPEMData(result) {
				t.Errorf("expected PEM output, got: %q", result[:80])
			}
			block, _ := pem.Decode([]byte(result))
			if block == nil {
				t.Fatal("failed to decode PEM block")
			}
			if tt.key == "Priv" {
				_, err = x509.ParsePKCS8PrivateKey(block.Bytes)
				if err != nil {
					t.Errorf("failed to parse private key: %v", err)
				}
			} else {
				_, err = x509.ParseCertificate(block.Bytes)
				if err != nil {
					t.Errorf("failed to parse certificate: %v", err)
				}
			}
		})
	}
}

func TestUnwrapBase64Layers_DoubleBase64PEM(t *testing.T) {
	certPEM, keyPEM, _, _ := generateTestCertPEM(t)

	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"cert base64(base64(PEM))", "Cert", base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte(certPEM))))},
		{"priv base64(base64(PEM))", "Priv", base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte(keyPEM))))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := unwrapBase64Layers(tt.val, tt.key)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !isPEMData(result) {
				t.Errorf("expected PEM output, got: %q", result[:80])
			}
		})
	}
}

func TestUnwrapBase64Layers_NotBase64(t *testing.T) {
	result, err := unwrapBase64Layers("not-base64!!!", "Cert")
	if err == nil {
		t.Errorf("expected error for invalid data, got result: %q", result)
	}
}

func TestUnwrapBase64Layers_PEMWithCRLF(t *testing.T) {
	certPEM, _, _, _ := generateTestCertPEM(t)
	crlfPEM := "\r\n" + certPEM + "\r\nextra\r\n"
	result, err := unwrapBase64Layers(crlfPEM, "Cert")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isPEMData(result) {
		t.Errorf("expected PEM output")
	}
}

func TestPemEncodeDER_CertKey(t *testing.T) {
	_, _, certDER, keyDER := generateTestCertPEM(t)

	result := pemEncodeDER(certDER, "Cert")
	if !isPEMData(result) {
		t.Fatal("expected PEM output for cert")
	}
	block, _ := pem.Decode([]byte(result))
	if block == nil {
		t.Fatal("failed to decode PEM block")
	}
	if block.Type != "CERTIFICATE" {
		t.Errorf("expected CERTIFICATE block type, got %s", block.Type)
	}
	_, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Errorf("failed to parse certificate: %v", err)
	}

	keyResult := pemEncodeDER(keyDER, "Priv")
	if !isPEMData(keyResult) {
		t.Fatal("expected PEM output for key")
	}
	keyBlock, _ := pem.Decode([]byte(keyResult))
	if keyBlock == nil {
		t.Fatal("failed to decode key PEM block")
	}
	_, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Errorf("failed to parse private key: %v", err)
	}
}

func TestPreprocessValue_NormalizesPEM(t *testing.T) {
	certPEM, _, _, _ := generateTestCertPEM(t)
	messed := certPEM + "\r\n"

	result := preprocessValue(messed, true)
	if result != normalizePEMData(messed) {
		t.Error("preprocessValue should normalize PEM secrets")
	}

	result = preprocessValue(messed, false)
	if result != messed {
		t.Error("preprocessValue should not normalize non-secrets")
	}
}
