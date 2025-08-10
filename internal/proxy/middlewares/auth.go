//go:build linux

package middlewares

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

type PBSAuth struct {
	privateKey *rsa.PrivateKey
}

func NewPBSAuth() (*PBSAuth, error) {
	keyData, err := os.ReadFile(constants.PBSAuthKeyPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(keyData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return &PBSAuth{privateKey: privateKey}, nil
}

func (p *PBSAuth) VerifyTicket(ticket string) (bool, error) {
	parts := strings.Split(ticket, "::")
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid ticket format")
	}

	ticketData := parts[0]
	signature := parts[1]

	sigBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return false, err
	}

	hasher := sha1.New()
	hasher.Write([]byte(ticketData))
	hashed := hasher.Sum(nil)

	err = rsa.VerifyPKCS1v15(&p.privateKey.PublicKey, crypto.SHA1, hashed, sigBytes)
	return err == nil, err
}

func (p *PBSAuth) CreateTicket(username, realm string) (string, error) {
	timestamp := time.Now().Unix()
	ticketData := fmt.Sprintf("PBS:%s@%s:%s", username, realm, hex.EncodeToString([]byte(strconv.FormatInt(timestamp, 16))))

	hasher := sha1.New()
	hasher.Write([]byte(ticketData))
	hashed := hasher.Sum(nil)

	signature, err := rsa.SignPKCS1v15(nil, p.privateKey, crypto.SHA1, hashed)
	if err != nil {
		return "", err
	}

	encodedSig := base64.StdEncoding.EncodeToString(signature)

	return fmt.Sprintf("%s::%s", ticketData, encodedSig), nil
}

func AgentOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := checkAgentAuth(store, r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}))
}

func ServerOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := checkProxyAuth(r); err != nil {
			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}))
}

func AgentOrServer(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	}))
}

func checkAgentAuth(store *store.Store, r *http.Request) error {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return fmt.Errorf("CheckAgentAuth: client certificate required")
	}

	clientCert := r.TLS.PeerCertificates[0]

	agentHostname := clientCert.Subject.CommonName
	if agentHostname == "" {
		return fmt.Errorf("CheckAgentAuth: missing certificate subject common name")
	}

	isWindows := true
	trustedCert, err := loadTrustedCert(store, agentHostname+" - C")
	if err != nil {
		isWindows = false
	}

	if !isWindows {
		trustedCert, err = loadTrustedCert(store, agentHostname+" - Root")
		if err != nil {
			return fmt.Errorf("CheckAgentAuth: certificate not trusted")
		}
	}

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

	auth, err := NewPBSAuth()
	if err != nil {
		return fmt.Errorf("CheckProxyAuth: failed to initialize PBS auth -> %w", err)
	}

	cookie, err := r.Cookie("__Host-PBSAuthCookie")
	if err != nil {
		// Fallback to legacy cookie
		cookie, err = r.Cookie("PBSAuthCookie")
		if err != nil {
			return fmt.Errorf("CheckProxyAuth: authentication required -> %w", err)
		}
	}

	// Verify the ticket
	valid, err := auth.VerifyTicket(cookie.Value)
	if err != nil || !valid {
		return fmt.Errorf("CheckProxyAuth: authentication required -> %w", err)
	}

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
