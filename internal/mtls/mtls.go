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
	"time"
)

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

func parseCACreds(caCertPEM, caKeyPEM []byte) (*x509.Certificate, *rsa.PrivateKey, error) {
	cb, _ := pem.Decode(caCertPEM)
	if cb == nil || cb.Type != "CERTIFICATE" {
		return nil, nil, errors.New("invalid CA cert PEM")
	}
	caCert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse CA cert: %w", err)
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

func BuildServerTLS(serverCertFile, serverKeyFile, caFile string, clientAuth tls.ClientAuthType) (*tls.Config, error) {
	if err := mustExist(serverCertFile, serverKeyFile, caFile); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(serverCertFile, serverKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server keypair: %w", err)
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("append client CA failed")
	}
	return &tls.Config{
		MinVersion:               tls.VersionTLS12,
		Certificates:             []tls.Certificate{cert},
		ClientCAs:                pool,
		ClientAuth:               clientAuth,
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}, nil
}

func BuildClientTLS(clientCertPEM, clientKeyPEM, caPEM []byte) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("append root CA failed")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
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

func EnsureLocalCAAndServerCert(outDir, org, caCN string, keySize, validDays int) (serverCert, serverKey, caCert, caKey string, err error) {
	interfaces, err := net.Interfaces()
	hostnames := []string{"localhost"}
	ips := []net.IP{net.ParseIP("127.0.0.1")}

	if err == nil {
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
		return "", "", "", "", fmt.Errorf("mkdir: %w", err)
	}

	caCertPath := filepath.Join(outDir, "ca.crt")
	caKeyPath := filepath.Join(outDir, "ca.key")
	serverCertPath := filepath.Join(outDir, "server.crt")
	serverKeyPath := filepath.Join(outDir, "server.key")

	if fileExists(serverCertPath) && fileExists(serverKeyPath) && fileExists(caCertPath) && fileExists(caKeyPath) {
		scPEM, sErr := os.ReadFile(serverCertPath)
		skPEM, kErr := os.ReadFile(serverKeyPath)
		caPEM, cErr := os.ReadFile(caCertPath)
		if sErr == nil && kErr == nil && cErr == nil {
			serverName := ""
			if len(hostnames) > 0 {
				serverName = hostnames[0]
			}
			if vErr := ValidateExistingCert(scPEM, skPEM, caPEM, serverName); vErr == nil {
				return serverCertPath, serverKeyPath, caCertPath, caKeyPath, nil
			}
		}
	}

	// CA
	caDER, caPriv, caX509, err := genCA(org, caCN, validDays, keySize)
	if err != nil {
		return "", "", "", "", err
	}
	if err = writePEM(caCertPath, "CERTIFICATE", caDER, 0o644); err != nil {
		return "", "", "", "", err
	}
	if err = writePEM(caKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caPriv), 0o600); err != nil {
		return "", "", "", "", err
	}

	// Server
	sk, err := rsa.GenerateKey(rand.Reader, keySize)
	if err != nil {
		return "", "", "", "", fmt.Errorf("server key: %w", err)
	}
	srvDER, err := signLeaf(caX509, caPriv, sk.Public(), pkix.Name{
		Organization: []string{org},
		CommonName:   "server",
	}, validDays, hostnames, ips, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		return "", "", "", "", err
	}
	if err = writePEM(serverCertPath, "CERTIFICATE", srvDER, 0o644); err != nil {
		return "", "", "", "", err
	}
	if err = writePEM(serverKeyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(sk), 0o600); err != nil {
		return "", "", "", "", err
	}

	return serverCertPath, serverKeyPath, caCertPath, caKeyPath, nil
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

func signLeaf(ca *x509.Certificate, caKey *rsa.PrivateKey, pub interface{}, subj pkix.Name, validDays int, dns []string, ips []net.IP, eku []x509.ExtKeyUsage) ([]byte, error) {
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
