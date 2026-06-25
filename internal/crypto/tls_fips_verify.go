package crypto

import (
	"crypto/x509"
	"fmt"
)


func VerifyCertSignatureFIPS(cert *x509.Certificate) error {
	switch cert.SignatureAlgorithm {
	case x509.SHA256WithRSA, x509.SHA384WithRSA, x509.SHA512WithRSA,
		x509.SHA256WithRSAPSS, x509.SHA384WithRSAPSS, x509.SHA512WithRSAPSS,
		x509.ECDSAWithSHA256, x509.ECDSAWithSHA384, x509.ECDSAWithSHA512:
		return nil
	default:
		return fmt.Errorf("crypto: certificate uses non-FIPS signature algorithm: %s", cert.SignatureAlgorithm)
	}
}
