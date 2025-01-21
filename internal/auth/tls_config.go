package auth

import (
	"crypto/tls"
	"crypto/x509"
	"time"
)

func GetClientTLSConfig(cert tls.Certificate, rootCAs *x509.CertPool) *tls.Config {
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			opts := x509.VerifyOptions{
				Roots:         rootCAs,
				CurrentTime:   time.Now(),
				Intermediates: x509.NewCertPool(),
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		},
	}
}
