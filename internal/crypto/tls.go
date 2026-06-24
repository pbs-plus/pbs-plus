package crypto

import (
	"crypto/tls"
	"crypto/x509"
)

var FIPSAllowedCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
}

var FIPSAllowedCurvePreferences = []tls.CurveID{
	tls.X25519,
	tls.CurveP256,
	tls.CurveP384,
}

func FIPSServerTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		CipherSuites:             FIPSAllowedCipherSuites,
		CurvePreferences:         FIPSAllowedCurvePreferences,
	}
}

func FIPSClientTLSConfig(minVersion uint16) *tls.Config {
	if minVersion < tls.VersionTLS12 {
		minVersion = tls.VersionTLS12
	}
	return &tls.Config{
		MinVersion:       minVersion,
		CipherSuites:     FIPSAllowedCipherSuites,
		CurvePreferences: FIPSAllowedCurvePreferences,
	}
}

var FIPSAllowedSignatureSchemes = []tls.SignatureScheme{
	tls.PKCS1WithSHA256,
	tls.PKCS1WithSHA384,
	tls.PKCS1WithSHA512,
	tls.PSSWithSHA256,
	tls.PSSWithSHA384,
	tls.PSSWithSHA512,
	tls.ECDSAWithP256AndSHA256,
	tls.ECDSAWithP384AndSHA384,
	tls.ECDSAWithP521AndSHA512,
}

func VerifyClientCertFIPS(certs []*x509.Certificate) error {
	if len(certs) == 0 {
		return errNoPeerCerts
	}
	for _, cert := range certs {
		if err := verifyCertSignatureFIPS(cert); err != nil {
			return err
		}
	}
	return nil
}
