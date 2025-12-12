package mtls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var currentTLSCert = atomic.Pointer[tls.Certificate]{}
var currentTLSCAs = atomic.Pointer[x509.CertPool]{}
var lastTLSTimestamp int64

func updateServerCurrentCerts(serverCertFile, serverKeyFile, caFile, prevCaFile string) error {
	if err := mustExist(serverCertFile, serverKeyFile, caFile); err != nil {
		return err
	}

	invalidPrevCA := false
	if prevCaFile != "" {
		if _, err := os.Stat(prevCaFile); err != nil {
			invalidPrevCA = true
		}
	}
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return err
	}
	currentTLSCert.Store(&cert)

	activeCA, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(activeCA) {
		return fmt.Errorf("certs failed to append")
	}
	if !invalidPrevCA {
		prev, err := os.ReadFile(prevCaFile)
		if err == nil {
			_ = pool.AppendCertsFromPEM(prev)
		}
	}

	currentTLSCAs.Store(pool)

	return nil
}

func getCurrentServerTLSCerts(serverCertFile, serverKeyFile, caFile, prevCaFile string) (*tls.Certificate, *x509.CertPool) {
	lastTLSTime := time.Unix(atomic.LoadInt64(&lastTLSTimestamp), 0)
	if time.Since(lastTLSTime) > 12*time.Hour {
		updateServerCurrentCerts(serverCertFile, serverKeyFile, caFile, prevCaFile)
	}

	return currentTLSCert.Load(), currentTLSCAs.Load()
}

func GenerateCSR(commonName string, keySize int) ([]byte, []byte, error) {
	if keySize <= 0 {
		keySize = 2048
	}
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}

	tpl := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: commonName,
		},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, tpl, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CSR: %w", err)
	}

	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return csrPEM, keyPEM, nil
}

func SignCSR(caCertPEM, caKeyPEM, csrPEM []byte, validDays int, extKeyUsages []x509.ExtKeyUsage) ([]byte, error) {
	if validDays <= 0 {
		validDays = 365
	}

	caCert, caKey, err := parseCACreds(caCertPEM, caKeyPEM)
	if err != nil {
		return nil, err
	}

	csrBlock, _ := pem.Decode(csrPEM)
	if csrBlock == nil || csrBlock.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("invalid CSR PEM")
	}
	csrObj, err := x509.ParseCertificateRequest(csrBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CSR: %w", err)
	}
	if err := csrObj.CheckSignature(); err != nil {
		return nil, fmt.Errorf("CSR signature check failed: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}
	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csrObj.Subject,
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(time.Duration(validDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  extKeyUsages,
		DNSNames:     csrObj.DNSNames,
		IPAddresses:  csrObj.IPAddresses,
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, csrObj.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}

func ParseCertInfo(caCertPEM []byte) (*x509.Certificate, error) {
	cb, _ := pem.Decode(caCertPEM)
	if cb == nil || cb.Type != "CERTIFICATE" {
		return nil, errors.New("invalid CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA cert: %w", err)
	}

	return caCert, nil
}

func parseCACreds(caCertPEM, caKeyPEM []byte) (*x509.Certificate, *rsa.PrivateKey, error) {
	caCert, err := ParseCertInfo(caCertPEM)
	if err != nil {
		return nil, nil, err
	}
	kb, _ := pem.Decode(caKeyPEM)
	if kb == nil || kb.Type != "RSA PRIVATE KEY" {
		return nil, nil, errors.New("invalid CA key PEM")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(kb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA key: %w", err)
	}
	return caCert, caKey, nil
}

func BuildServerTLS(serverCertFile, serverKeyFile, caFile, prevCaFile string, clientAuth tls.ClientAuthType) (*tls.Config, error) {
	if err := updateServerCurrentCerts(serverCertFile, serverKeyFile, caFile, prevCaFile); err != nil {
		return nil, err
	}

	return &tls.Config{
		GetConfigForClient: func(_ *tls.ClientHelloInfo) (*tls.Config, error) {
			currentCerts, currentCAs := getCurrentServerTLSCerts(serverCertFile, serverKeyFile, caFile, prevCaFile)
			return &tls.Config{
				MinVersion:               tls.VersionTLS13,
				Certificates:             []tls.Certificate{*currentCerts},
				NextProtos:               []string{"h2", "pbsarpc"},
				ClientCAs:                currentCAs,
				ClientAuth:               clientAuth,
				PreferServerCipherSuites: true,
				CipherSuites: []uint16{
					tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
					tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
					tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
				},
			}, nil
		},
	}, nil
}

func BuildClientTLS(clientCertPEM, clientKeyPEM, caPEM []byte, legacyCaPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("append active root CA failed")
	}
	if legacyCaPEM != nil {
		_ = rootCAs.AppendCertsFromPEM(legacyCaPEM)
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2", "pbsarpc"},
		RootCAs:      rootCAs,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		},
	}, nil
}

func ValidateExistingCert(certPEM, keyPEM, caPEM []byte, serverName string) error {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return fmt.Errorf("parse keypair: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return errors.New("no certificate found in keypair")
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse leaf: %w", err)
	}

	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return fmt.Errorf("certificate not valid before: %s", leaf.NotBefore)
	}
	if now.After(leaf.NotAfter) {
		return fmt.Errorf("certificate expired at: %s", leaf.NotAfter)
	}

	if len(caPEM) > 0 {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			return errors.New("failed to append CA to roots")
		}

		intermediates := x509.NewCertPool()
		if len(cert.Certificate) > 1 {
			for _, der := range cert.Certificate[1:] {
				c := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
				_ = intermediates.AppendCertsFromPEM(c)
			}
		}

		opts := x509.VerifyOptions{
			Roots:         roots,
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		if serverName != "" {
			opts.DNSName = serverName
		}
		if _, err := leaf.Verify(opts); err != nil {
			return fmt.Errorf("verify against CA failed: %w", err)
		}
	} else if serverName != "" {
		if err := leaf.VerifyHostname(serverName); err != nil {
			return fmt.Errorf("hostname verification failed: %w", err)
		}
	}

	return nil
}

func EnsureLocalCA(outDir, org, caCN string, keySize, validDays int) (caCertPath, caKeyPath string, err error) {
	if outDir == "" {
		outDir = "./certs"
	}
	if keySize <= 0 {
		keySize = 2048
	}
	if validDays <= 0 {
		validDays = 365
	}
	if err = os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir: %w", err)
	}

	caCertPath = filepath.Join(outDir, filepath.Base(constants.AgentTLSCACertFile))
	caKeyPath = filepath.Join(outDir, filepath.Base(constants.AgentTLSCAKeyFile))
	prevCACert := filepath.Join(outDir, filepath.Base(constants.AgentTLSPrevCAKeyFile))

	if fileExists(caCertPath) && fileExists(caKeyPath) {
		caPEM, rErr := os.ReadFile(caCertPath)
		keyPEM, kErr := os.ReadFile(caKeyPath)
		if rErr == nil && kErr == nil {
			caCert, _, vErr := parseCACreds(caPEM, keyPEM)
			if vErr == nil {
				timeUntilExpiry := time.Until(caCert.NotAfter)
				if timeUntilExpiry <= constants.TLSCARotationGraceDays*24*time.Hour {
					if err := os.Rename(caCertPath, prevCACert); err != nil {
						return "", "", fmt.Errorf("rotate: move current CA to previous: %w", err)
					}
				}
				return caCertPath, caKeyPath, nil
			}
		}
	}

	caDER, caPriv, _, err := genCA(org, caCN, validDays, keySize)
	if err != nil {
		return "", "", err
	}
	if err = writePEM(caCertPath, "CERTIFICATE", caDER, 0o644); err != nil {
		return "", "", err
	}
	if err = writePEM(caKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caPriv), 0o600); err != nil {
		return "", "", err
	}

	return caCertPath, caKeyPath, nil
}

func EnsureServerCertUsingExistingCA(outDir, org string, keySize, validDays int, caCertPath, caKeyPath string) (serverCertPath, serverKeyPath string, err error) {
	if outDir == "" {
		outDir = "./certs"
	}
	if keySize <= 0 {
		keySize = 2048
	}
	if validDays <= 0 {
		validDays = 365
	}
	if err = os.MkdirAll(outDir, 0o755); err != nil {
		return "", "", fmt.Errorf("mkdir: %w", err)
	}

	if caCertPath == "" || caKeyPath == "" {
		return "", "", errors.New("CA paths must be provided")
	}
	if err := mustExist(caCertPath, caKeyPath); err != nil {
		return "", "", err
	}

	serverCertPath = filepath.Join(outDir, "server.crt")
	serverKeyPath = filepath.Join(outDir, "server.key")

	hostnames := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}

	interfaces, ifErr := net.Interfaces()
	if ifErr == nil {
		for _, i := range interfaces {
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

	if hostname, err := os.Hostname(); err == nil {
		hostnames = append(hostnames, hostname)
	}

	userHostname, ok := os.LookupEnv("PBS_PLUS_HOSTNAME")
	if ok {
		if err := utils.ValidateHostname(userHostname); err == nil {
			hostnames = append(hostnames, userHostname)
		}
	}

	if fileExists(serverCertPath) && fileExists(serverKeyPath) {
		scPEM, sErr := os.ReadFile(serverCertPath)
		skPEM, kErr := os.ReadFile(serverKeyPath)
		caPEM, cErr := os.ReadFile(caCertPath)
		if sErr == nil && kErr == nil && cErr == nil {
			serverName := os.Getenv("PBS_PLUS_HOSTNAME")
			if vErr := ValidateExistingCert(scPEM, skPEM, caPEM, serverName); vErr == nil {
				return serverCertPath, serverKeyPath, nil
			}
		}
	}

	caCertPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return "", "", fmt.Errorf("read CA cert: %w", err)
	}
	caKeyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return "", "", fmt.Errorf("read CA key: %w", err)
	}
	cb, _ := pem.Decode(caCertPEM)
	if cb == nil || cb.Type != "CERTIFICATE" {
		return "", "", errors.New("invalid CA cert PEM")
	}
	caX509, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA x509: %w", err)
	}
	kb, _ := pem.Decode(caKeyPEM)
	if kb == nil || kb.Type != "RSA PRIVATE KEY" {
		return "", "", errors.New("invalid CA key PEM")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(kb.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("parse CA key: %w", err)
	}

	sk, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return "", "", fmt.Errorf("server key: %w", err)
	}
	srvDER, err := signLeaf(caX509, caKey, sk.Public(), pkix.Name{
		Organization: []string{org},
		CommonName:   "server",
	}, validDays, hostnames, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		return "", "", err
	}
	if err = writePEM(serverCertPath, "CERTIFICATE", srvDER, 0o644); err != nil {
		return "", "", err
	}
	if err = writePEM(serverKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(sk), 0o600); err != nil {
		return "", "", err
	}

	return serverCertPath, serverKeyPath, nil
}

func EnsureLocalCAAndServerCert(outDir, org, caCN string, keySize, validDays int) (serverCert, serverKey, caCert, caKey string, err error) {
	caCert, caKey, err = EnsureLocalCA(outDir, org, caCN, keySize, validDays)
	if err != nil {
		return "", "", "", "", err
	}
	serverCert, serverKey, err = EnsureServerCertUsingExistingCA(outDir, org, keySize, validDays, caCert, caKey)
	if err != nil {
		return "", "", "", "", err
	}
	return serverCert, serverKey, caCert, caKey, nil
}

func genCA(org, cn string, validDays, keySize int) ([]byte, *rsa.PrivateKey, *x509.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("generate CA key: %w", err)
	}
	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{org},
			CommonName:   cn,
		},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(time.Duration(validDays) * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLen:            1,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create CA cert: %w", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse CA cert: %w", err)
	}
	return der, key, ca, nil
}

func signLeaf(ca *x509.Certificate, caKey *rsa.PrivateKey, pub any, subj pkix.Name, validDays int, dns []string, ips []net.IP, eku []x509.ExtKeyUsage) ([]byte, error) {
	now := time.Now()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, fmt.Errorf("serial: %w", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      subj,
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(time.Duration(validDays) * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  eku,
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, ca, pub, caKey)
	if err != nil {
		return nil, fmt.Errorf("create leaf cert: %w", err)
	}
	return der, nil
}

func writePEM(path, typ string, der []byte, mode os.FileMode) error {
	b := pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der})
	return os.WriteFile(path, b, mode)
}

func mustExist(paths ...string) error {
	for _, p := range paths {
		if p == "" {
			return fmt.Errorf("path is empty")
		}
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("missing file %s: %w", p, err)
		}
	}
	return nil
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}
