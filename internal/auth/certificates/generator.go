package certificates

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
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
	return &Options{
		Organization: "PBS Plus",
		CommonName:   "PBS Plus CA",
		ValidDays:    365,
		KeySize:      2048,
		OutputDir:    "certs",
		Hostnames:    []string{"localhost"},
		IPs:          []net.IP{net.ParseIP("127.0.0.1")},
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
	certOut, err := os.Create(filepath.Join(g.options.OutputDir, filename))
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
	keyOut, err := os.OpenFile(
		filepath.Join(g.options.OutputDir, filename),
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0600,
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
