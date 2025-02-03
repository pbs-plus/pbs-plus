//go:build linux

package middlewares

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"

	"github.com/sonroyaalmerol/pbs-plus/internal/store"
)

func AgentOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := checkAgentAuth(store, r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func ServerOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := checkProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func AgentOrServer(store *store.Store, next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authenticated := false

		if err := checkAgentAuth(store, r); err == nil {
			authenticated = true
		}

		if err := checkProxyAuth(r); err == nil {
			authenticated = true
		}

		if !authenticated {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}
}

func checkAgentAuth(store *store.Store, r *http.Request) error {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return fmt.Errorf("CheckAgentAuth: client certificate required")
	}

	agentHostname := r.Header.Get("X-PBS-Agent")
	if agentHostname == "" {
		return fmt.Errorf("CheckAgentAuth: missing X-PBS-Agent header")
	}

	trustedCert, err := loadTrustedCert(store, agentHostname+" - C")
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: certificate not trusted")
	}

	clientCert := r.TLS.PeerCertificates[0]

	if !clientCert.Equal(trustedCert) {
		return fmt.Errorf("certificate does not match pinned certificate")
	}

	return nil
}

func checkProxyAuth(r *http.Request) error {
	agentHostname := r.Header.Get("X-PBS-Agent")
	if agentHostname != "" {
		return fmt.Errorf("CheckProxyAuth: agent unauthorized")
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

func loadTrustedCert(store *store.Store, targetName string) (*x509.Certificate, error) {
	target, err := store.Database.GetTarget(targetName)
	if err != nil {
		return nil, fmt.Errorf("failed to get target: %w", err)
	}

	decodedCert, err := base64.StdEncoding.DecodeString(target.Auth)
	if err != nil {
		return nil, fmt.Errorf("failed to get target cert: %w", err)
	}

	block, _ := pem.Decode(decodedCert)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %v", err)
	}

	return cert, nil
}
