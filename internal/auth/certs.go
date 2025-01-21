package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

const (
	caCertPath = "/etc/proxmox-backup/pbs-plus/ca.crt"
	caKeyPath  = "/etc/proxmox-backup/pbs-plus/ca.key"
)

type CertGenerator struct {
	CA        *x509.Certificate
	CAPrivKey *rsa.PrivateKey
}

func loadExistingCA() (*x509.Certificate, *rsa.PrivateKey, error) {
	// Read CA certificate
	certPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	// Decode PEM block
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("failed to decode CA certificate PEM")
	}

	// Parse certificate
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	// Read private key
	keyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read CA private key: %w", err)
	}

	// Decode PEM block
	block, _ = pem.Decode(keyPEM)
	if block == nil {
		return nil, nil, fmt.Errorf("failed to decode CA private key PEM")
	}

	// Parse private key
	privKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA private key: %w", err)
	}

	return cert, privKey, nil
}

func generateNewCA() (*x509.Certificate, *rsa.PrivateKey, error) {
	// Generate CA key pair
	CAPrivKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate CA private key: %w", err)
	}

	// Create CA certificate
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject: pkix.Name{
			Organization: []string{"PBS Plus"},
			CommonName:   "PBS Plus",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years validity
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// Self-sign CA certificate
	caCertBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &CAPrivKey.PublicKey, CAPrivKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}

	// Parse the generated certificate
	ca, err = x509.ParseCertificate(caCertBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	// Ensure directory exists
	err = os.MkdirAll(filepath.Dir(caCertPath), 0755)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Save CA certificate
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCertBytes,
	})
	err = os.WriteFile(caCertPath, certPEM, 0644)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to save CA certificate: %w", err)
	}

	// Save CA private key
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(CAPrivKey),
	})
	err = os.WriteFile(caKeyPath, keyPEM, 0600)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to save CA private key: %w", err)
	}

	return ca, CAPrivKey, nil
}

func NewCertGenerator() (*CertGenerator, error) {
	ca, CAPrivKey, err := loadExistingCA()
	if err != nil {
		ca, CAPrivKey, err = generateNewCA()
		if err != nil {
			return nil, fmt.Errorf("failed to generate new CA: %w", err)
		}
	}

	return &CertGenerator{
		CA:        ca,
		CAPrivKey: CAPrivKey,
	}, nil
}

func (cg *CertGenerator) GenerateCert(csrPEM string) (certPEM []byte, err error) {
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("Invalid CSR")
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse CSR")
	}

	// Verify CSR signature
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("Invalid CSR signature")
	}

	// Create certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().Unix()),
		Subject:      csr.Subject,
		NotBefore:    time.Now(),
		NotAfter:     time.Now().AddDate(1, 0, 0), // 1 year validity
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	// Create certificate
	certDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		cg.CA,
		csr.PublicKey,
		cg.CAPrivKey,
	)
	if err != nil {
		return nil, fmt.Errorf("Failed to create certificate")
	}

	// Encode certificate to PEM
	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	})

	return
}
