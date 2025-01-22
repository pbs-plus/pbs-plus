package certificates

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	authErrors "github.com/sonroyaalmerol/pbs-plus/internal/auth/errors"
)

// Options represents configuration for certificate generation
type Options struct {
	// Organization name for the CA certificate
	Organization string
	// Common name for the CA certificate
	CommonName string
	// Valid duration for certificates
	ValidDays int
	// Key size in bits (e.g., 2048, 4096)
	KeySize int
	// Output directory for certificates
	OutputDir string
	// Hostnames to include in SAN
	Hostnames []string
	// IP addresses to include in SAN
	IPs []net.IP
}

// DefaultOptions returns default certificate generation options
func DefaultOptions() *Options {
	// Get all non-loopback interfaces
	interfaces, err := net.Interfaces()
	hostnames := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}

	if err == nil {
		for _, i := range interfaces {
			// Skip loopback
			if i.Flags&net.FlagLoopback != 0 {
				continue
			}
			addrs, err := i.Addrs()
			if err != nil {
				continue
			}
			for _, addr := range addrs {
				switch v := addr.(type) {
				case *net.IPNet:
					if ip4 := v.IP.To4(); ip4 != nil {
						ips = append(ips, ip4)
					}
				}
			}
		}
	}

	// Try to get hostname
	if hostname, err := os.Hostname(); err == nil {
		hostnames = append(hostnames, hostname)
	}

	return &Options{
		Organization: "PBS Plus",
		CommonName:   "PBS Plus CA",
		ValidDays:    365,
		KeySize:      2048,
		OutputDir:    "/etc/proxmox-backup/pbs-plus/certs",
		Hostnames:    hostnames,
		IPs:          ips,
	}
}

// Generator handles certificate generation
type Generator struct {
	options *Options
	ca      *x509.Certificate
	caKey   *rsa.PrivateKey
}

// NewGenerator creates a new certificate generator
func NewGenerator(options *Options) (*Generator, error) {
	if options == nil {
		options = DefaultOptions()
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(options.OutputDir, 0755); err != nil {
		return nil, authErrors.WrapError("create_output_dir", err)
	}

	return &Generator{
		options: options,
	}, nil
}

func (g *Generator) GetCAPEM() []byte {
	return EncodeCertPEM(g.ca.Raw)
}

// GenerateCA generates a new CA certificate and private key
func (g *Generator) GenerateCA() error {
	key, err := rsa.GenerateKey(rand.Reader, g.options.KeySize)
	if err != nil {
		return authErrors.WrapError("generate_ca_key", err)
	}

	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{g.options.Organization},
			CommonName:   g.options.CommonName,
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(0, 0, g.options.ValidDays),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	// Self-sign the CA certificate
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		return authErrors.WrapError("create_ca_cert", err)
	}

	// Save CA certificate
	if err := g.saveCertificate("ca.crt", caBytes); err != nil {
		return err
	}

	// Save CA private key
	if err := g.savePrivateKey("ca.key", key); err != nil {
		return err
	}

	g.ca = ca
	g.caKey = key
	return nil
}

// GenerateCert generates a new certificate signed by the CA
func (g *Generator) GenerateCert(name string) error {
	if g.ca == nil || g.caKey == nil {
		return authErrors.WrapError("generate_cert",
			errors.New("CA must be generated first"))
	}

	key, err := rsa.GenerateKey(rand.Reader, g.options.KeySize)
	if err != nil {
		return authErrors.WrapError("generate_key", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization: []string{g.options.Organization},
			CommonName:   name,
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(0, 0, g.options.ValidDays),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:    g.options.Hostnames,
		IPAddresses: g.options.IPs,
	}

	// Sign the certificate with CA
	certBytes, err := x509.CreateCertificate(rand.Reader, template, g.ca, &key.PublicKey, g.caKey)
	if err != nil {
		return authErrors.WrapError("create_cert", err)
	}

	// Save certificate
	if err := g.saveCertificate(name+".crt", certBytes); err != nil {
		return err
	}

	// Save private key
	if err := g.savePrivateKey(name+".key", key); err != nil {
		return err
	}

	return nil
}

// GenerateAll generates CA and all required certificates
func (g *Generator) GenerateAll() error {
	if err := g.GenerateCA(); err != nil {
		return err
	}

	if err := g.GenerateCert("server"); err != nil {
		return err
	}

	if err := g.GenerateCert("agent"); err != nil {
		return err
	}

	return nil
}

func (g *Generator) saveCertificate(filename string, certBytes []byte) error {
	filePath := filepath.Join(g.options.OutputDir, filename)

	certOut, err := os.Create(filePath)
	if err != nil {
		return authErrors.WrapError("create_cert_file", err)
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certBytes,
	}); err != nil {
		return authErrors.WrapError("encode_cert", err)
	}

	return nil
}

func (g *Generator) savePrivateKey(filename string, key *rsa.PrivateKey) error {
	filePath := filepath.Join(g.options.OutputDir, filename)
	keyOut, err := os.OpenFile(
		filePath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0640,
	)
	if err != nil {
		return authErrors.WrapError("create_key_file", err)
	}
	defer keyOut.Close()

	if err := pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}); err != nil {
		return authErrors.WrapError("encode_key", err)
	}

	return nil
}

func (g *Generator) ValidateExistingCerts() error {
	serverCertPath := filepath.Join(g.options.OutputDir, "server.crt")
	caPath := filepath.Join(g.options.OutputDir, "ca.crt")
	caKeyPath := filepath.Join(g.options.OutputDir, "ca.key")

	// Check if files exist
	if _, err := os.Stat(serverCertPath); os.IsNotExist(err) {
		return fmt.Errorf("server certificate not found: %s", serverCertPath)
	}
	if _, err := os.Stat(caPath); os.IsNotExist(err) {
		return fmt.Errorf("CA certificate not found: %s", caPath)
	}
	if _, err := os.Stat(caKeyPath); os.IsNotExist(err) {
		return fmt.Errorf("CA certificate not found: %s", caPath)
	}

	// Load server certificate
	serverCertPEM, err := os.ReadFile(serverCertPath)
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

	// Load CA certificate
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("failed to read CA certificate: %w", err)
	}
	block, _ = pem.Decode(caPEM)
	if block == nil {
		return fmt.Errorf("failed to parse CA certificate PEM")
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	g.ca = caCert

	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return fmt.Errorf("failed to read CA key: %w", err)
	}
	block, _ = pem.Decode(caKeyPEM)
	if block == nil {
		return fmt.Errorf("failed to parse CA key PEM")
	}

	caKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse CA key: %w", err)
	}

	g.caKey = caKey

	// Verify server certificate is signed by CA
	roots := x509.NewCertPool()
	roots.AddCert(caCert)
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
	if now.Before(serverCert.NotBefore) {
		return fmt.Errorf("server certificate is not yet valid")
	}
	if now.After(serverCert.NotAfter) {
		return fmt.Errorf("server certificate has expired")
	}
	if now.Before(caCert.NotBefore) {
		return fmt.Errorf("CA certificate is not yet valid")
	}
	if now.After(caCert.NotAfter) {
		return fmt.Errorf("CA certificate has expired")
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
	if g.caKey == nil {
		return nil, fmt.Errorf("CA private key is nil")
	}
	if g.ca == nil {
		return nil, fmt.Errorf("CA certificate is nil")
	}
	if g.options == nil {
		return nil, fmt.Errorf("options are nil")
	}

	validDays := g.options.ValidDays
	if validDays <= 0 {
		return nil, fmt.Errorf("invalid validity period: %d days", validDays)
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
		NotAfter:     time.Now().AddDate(0, 0, validDays),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, g.ca, csrObj.PublicKey, g.caKey)
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
