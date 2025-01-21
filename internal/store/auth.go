//go:build linux

package store

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"time"
)

func checkAgentAuth(storeInstance *Store, r *http.Request) error {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return fmt.Errorf("CheckAgentAuth: client certificate required")
	}

	agentHostname := r.Header.Get("X-PBS-Agent")
	if agentHostname == "" {
		return fmt.Errorf("CheckAgentAuth: missing X-PBS-Agent header")
	}

	targetEncoded, err := storeInstance.GetTarget(fmt.Sprintf("%s - C", strings.TrimSpace(agentHostname)))
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: failed to get target: %w", err)
	}

	storedCertPEM, err := base64.StdEncoding.DecodeString(targetEncoded.Auth)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: invalid stored cert")
	}

	block, _ := pem.Decode(storedCertPEM)
	if block == nil {
		return fmt.Errorf("CheckAgentAuth: failed to decode stored certificate PEM")
	}

	storedCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: failed to parse stored certificate: %w", err)
	}

	clientCert := r.TLS.PeerCertificates[0]

	if !clientCert.NotBefore.Equal(storedCert.NotBefore) ||
		!clientCert.NotAfter.Equal(storedCert.NotAfter) ||
		clientCert.SerialNumber.Cmp(storedCert.SerialNumber) != 0 ||
		clientCert.Subject.CommonName != storedCert.Subject.CommonName {
		return fmt.Errorf("CheckAgentAuth: certificate mismatch")
	}

	if time.Now().After(clientCert.NotAfter) {
		return fmt.Errorf("CheckAgentAuth: certificate has expired")
	}

	roots := x509.NewCertPool()
	roots.AddCert(storeInstance.CertGenerator.CA)
	opts := x509.VerifyOptions{
		Roots:         roots,
		CurrentTime:   time.Now(),
		Intermediates: x509.NewCertPool(),
	}
	_, err = clientCert.Verify(opts)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: certificate verification failed: %w", err)
	}

	return nil
}

func (storeInstance *Store) CheckProxyAuth(r *http.Request) error {
	agentHostname := r.Header.Get("X-PBS-Agent")
	if strings.TrimSpace(agentHostname) != "" {
		return checkAgentAuth(storeInstance, r)
	}

	// checkEndpoint := "/api2/json/version"
	// req, err := http.NewRequest(
	// 	http.MethodGet,
	// 	fmt.Sprintf(
	// 		"%s%s",
	// 		ProxyTargetURL,
	// 		checkEndpoint,
	// 	),
	// 	nil,
	// )

	// if err != nil {
	// 	return fmt.Errorf("CheckProxyAuth: error creating http request -> %w", err)
	// }

	// for _, cookie := range r.Cookies() {
	// 	req.AddCookie(cookie)
	// }

	// if authHead := r.Header.Get("Authorization"); authHead != "" {
	// 	req.Header.Set("Authorization", authHead)
	// }

	// if storeInstance.HTTPClient == nil {
	// 	storeInstance.HTTPClient = &http.Client{
	// 		Timeout:   time.Second * 30,
	// 		Transport: utils.BaseTransport,
	// 	}
	// }

	// resp, err := storeInstance.HTTPClient.Do(req)
	// if err != nil {
	// 	return fmt.Errorf("CheckProxyAuth: invalid auth -> %w", err)
	// }
	// defer func() {
	// 	_, _ = io.Copy(io.Discard, resp.Body)
	// 	resp.Body.Close()
	// }()

	// if resp.StatusCode > 299 || resp.StatusCode < 200 {
	// 	return fmt.Errorf("CheckProxyAuth: invalid auth -> %w", err)
	// }

	return nil
}
