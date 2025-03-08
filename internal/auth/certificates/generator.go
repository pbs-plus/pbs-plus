package certificates

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math"
	"math/big"
	"os"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/constants"
)

// Generator handles certificate generation
type Generator struct {
	serverCert *x509.Certificate
	serverKey  *rsa.PrivateKey
}

// NewGenerator creates a new certificate generator
func NewGenerator() (*Generator, error) {
	return &Generator{}, nil
}

func (g *Generator) GetCertificatePEM() []byte {
	return EncodeCertPEM(g.serverCert.Raw)
}

func (g *Generator) ValidateExistingCerts() error {
	// Check if files exist
	if _, err := os.Stat(constants.PBSCert); os.IsNotExist(err) {
		return fmt.Errorf("server certificate not found: %s", constants.PBSCert)
	}
	if _, err := os.Stat(constants.PBSKey); os.IsNotExist(err) {
		return fmt.Errorf("server certificate key not found: %s", constants.PBSKey)
	}

	// Load server certificate
	serverCertPEM, err := os.ReadFile(constants.PBSCert)
	if err != nil {
		return fmt.Errorf("failed to read server certificate: %w", err)
	}
	block, _ := pem.Decode(serverCertPEM)
	if block == nil {
		return fmt.Errorf("failed to parse server certificate PEM")
	}
	serverCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse server certificate: %w", err)
	}

	g.serverCert = serverCert

	serverKeyPEM, err := os.ReadFile(constants.PBSKey)
	if err != nil {
		return fmt.Errorf("failed to read server key: %w", err)
	}
	block, _ = pem.Decode(serverKeyPEM)
	if block == nil {
		return fmt.Errorf("failed to parse server key PEM")
	}

	serverKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse server key: %w", err)
	}

	g.serverKey = serverKey

	// Verify server certificate is signed by CA
	roots := x509.NewCertPool()
	roots.AddCert(serverCert)
	opts := x509.VerifyOptions{
		Roots: roots,
		KeyUsages: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
	}

	if _, err := serverCert.Verify(opts); err != nil {
		return fmt.Errorf("server certificate validation failed: %w", err)
	}

	// Check certificate expiry
	now := time.Now()
	if now.After(serverCert.NotAfter) {
		return fmt.Errorf("server certificate has expired, you will need to rebootstrap all your agents")
	}

	return nil
}

func GenerateCSR(commonName string, keySize int) ([]byte, *rsa.PrivateKey, error) {
	privKey, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate private key: %w", err)
	}

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: commonName,
		},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, template, privKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CSR: %w", err)
	}

	return csrBytes, privKey, nil
}

func (g *Generator) SignCSR(csr []byte) ([]byte, error) {
	if g == nil {
		return nil, fmt.Errorf("generator is nil")
	}
	if g.serverCert == nil {
		return nil, fmt.Errorf("server certificate is nil")
	}
	if g.serverKey == nil {
		return nil, fmt.Errorf("server private key is nil")
	}

	// Parse CSR
	csrObj, err := x509.ParseCertificateRequest(csr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSR: %w", err)
	}
	if err := csrObj.CheckSignature(); err != nil {
		return nil, fmt.Errorf("CSR signature check failed: %w", err)
	}

	// Validate public key
	if csrObj.PublicKey == nil {
		return nil, fmt.Errorf("CSR public key is nil")
	}

	serialNumber, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject:      csrObj.Subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(0, 0, 90),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, g.serverCert, csrObj.PublicKey, g.serverKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create certificate: %w", err)
	}

	return EncodeCertPEM(certBytes), nil
}

// Helper functions to encode to PEM
func EncodeCertPEM(cert []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert,
	})
}

func EncodeKeyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

func EncodeCSRPEM(csr []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csr,
	})
}
