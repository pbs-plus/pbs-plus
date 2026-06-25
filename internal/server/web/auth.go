//go:build linux

package web

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/store"
)

func getClientInfo(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ips := strings.Split(xff, ","); len(ips) > 0 {
			return strings.TrimSpace(ips[0])
		}
	}

	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type PBSAuth struct {
	privateKey crypto.PrivateKey
	keyType    string
}

var (
	cachedPBSAuth *PBSAuth
	pbsAuthOnce   sync.Once
	pbsAuthErr    error
)

func GetPBSAuth() (*PBSAuth, error) {
	pbsAuthOnce.Do(func() {
		cachedPBSAuth, pbsAuthErr = NewPBSAuth()
		if pbsAuthErr != nil {
			log.Error(fmt.Errorf("GetPBSAuth: failed to initialize PBS auth at startup: %w", pbsAuthErr), "")
		}
	})
	return cachedPBSAuth, pbsAuthErr
}

func NewPBSAuth() (*PBSAuth, error) {
	keyData, err := os.ReadFile(conf.PBSAuthKeyPath)
	if err != nil {
		return nil, err
	}

	block, _ := pem.Decode(keyData)
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		rsaKey, rsaErr := x509.ParsePKCS1PrivateKey(block.Bytes)
		if rsaErr != nil {
			return nil, fmt.Errorf("failed to parse private key as PKCS#8: %w; as PKCS#1: %w", err, rsaErr)
		}
		return &PBSAuth{privateKey: rsaKey, keyType: "rsa"}, nil
	}

	switch key := privateKey.(type) {
	case ed25519.PrivateKey:
		return &PBSAuth{privateKey: key, keyType: "ed25519"}, nil
	case *rsa.PrivateKey:
		return &PBSAuth{privateKey: key, keyType: "rsa"}, nil
	default:
		return nil, fmt.Errorf("unsupported key type: %T", key)
	}
}

func (p *PBSAuth) VerifyTicket(cookieVal string) (bool, error) {
	left, sigStr, usedDecoded, err := splitPBS(cookieVal)
	if err != nil {
		log.Error(err, "", "has_percent", strings.Contains(cookieVal, "%"), "cookie_len", len(cookieVal), "stage", "split")

		return false, fmt.Errorf("invalid ticket format: %w", err)
	}

	if strings.HasPrefix(sigStr, ":") {
		log.Warn("VerifyTicket: signature had unexpected leading colon")
		sigStr = sigStr[1:]
	}

	sigStr = strings.Trim(sigStr, " \t")

	if strings.Contains(sigStr, " ") {
		sigStr = strings.ReplaceAll(sigStr, " ", "+")
	}

	sig, err := base64.RawStdEncoding.DecodeString(sigStr)
	if err != nil {
		if strings.ContainsAny(sigStr, "-_") && !strings.ContainsAny(sigStr, "+/") {
			sig, err = base64.RawURLEncoding.DecodeString(sigStr)
		}
		if err != nil {
			log.Error(fmt.Errorf("base64 decode failed: %w", err), "", "alphabet_hint",
				alphabetHint(sigStr), "sig_preview", preview(sigStr, 80), "sig_len", len(sigStr), "used_decoded_cookie", usedDecoded, "stage", "b64decode")

			return false, fmt.Errorf("signature base64 decode failed: %w", err)
		}
	}

	switch p.keyType {
	case "ed25519":
		pub := p.privateKey.(ed25519.PrivateKey).Public().(ed25519.PublicKey)
		if !ed25519.Verify(pub, []byte(left), sig) {
			log.Error(fmt.Errorf("ed25519 signature invalid"), "", "used_decoded_cookie", usedDecoded, "msg_prefix", preview(left, 32), "msg_len", len(left), "stage", "verify")

			return false, fmt.Errorf("ed25519 signature invalid")
		}
		return true, nil

	case "rsa":
		pub := &p.privateKey.(*rsa.PrivateKey).PublicKey
		sum := sha256.Sum256([]byte(left))
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig); err != nil {
			log.Error(fmt.Errorf("rsa verify failed: %w", err), "", "used_decoded_cookie", usedDecoded, "msg_prefix", preview(left, 32), "msg_len", len(left), "hash", "sha256", "stage", "verify")

			return false, fmt.Errorf("rsa signature invalid: %w", err)
		}
		return true, nil

	default:
		log.Error(fmt.Errorf("unsupported key type: %s", p.keyType), "", "stage", "verify")

		return false, fmt.Errorf("unsupported key type: %s", p.keyType)
	}
}

func splitPBS(raw string) (left, right string, usedDecoded bool, err error) {
	if parts := strings.SplitN(raw, "::", 2); len(parts) == 2 {
		return parts[0], parts[1], false, nil
	}
	if strings.Contains(raw, "%") {
		if parts := strings.SplitN(raw, "%3A%3A", 2); len(parts) == 2 {
			leftToVerify := parts[0]
			if strings.Contains(leftToVerify, "%") {
				if decoded, decErr := url.QueryUnescape(leftToVerify); decErr == nil {
					leftToVerify = decoded
				}
			}
			return leftToVerify, parts[1], false, nil
		}
	}
	return "", "", false, fmt.Errorf("missing '::' separator")
}

func preview(s string, n int) string {
	if len(s) > n {
		s = s[:n]
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 32 || c == 127 {
			b.WriteString(fmt.Sprintf("\\x%02X", c))
		} else {
			b.WriteByte(c)
		}
	}
	if len(s) == n {
		b.WriteString("...")
	}
	return b.String()
}

func alphabetHint(s string) string {
	hasPlus := strings.Contains(s, "+")
	hasSlash := strings.Contains(s, "/")
	hasDash := strings.Contains(s, "-")
	hasUnderscore := strings.Contains(s, "_")
	return fmt.Sprintf("+/=%t/%t, -_=%t/%t", hasPlus, hasSlash, hasDash, hasUnderscore)
}

func AgentOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostname, err := checkAgentAuth(store, r)
		if err != nil {
			log.Error(err, "", "hostname", getClientInfo(r), "mode", "agent_only")

			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		r.Header.Set("X-PBS-Authenticated-Agent", hostname)
		next.ServeHTTP(w, r)
	}))
}

func ServerOnly(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := checkProxyAuth(r); err != nil && !IsLocalhost(r) {
			log.Error(err, "", "hostname", getClientInfo(r), "mode", "server_only")

			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	}))
}

func AgentOrServer(store *store.Store, next http.Handler) http.HandlerFunc {
	return CORS(store, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authenticated := false
		var lastErr error

		hostname, agentErr := checkAgentAuth(store, r)
		if agentErr == nil {
			authenticated = true
		} else {
			lastErr = agentErr
		}

		if err := checkProxyAuth(r); err == nil || IsLocalhost(r) {
			authenticated = true
		} else {
			lastErr = err
		}

		if !authenticated {
			log.Error(lastErr, "", "hostname", getClientInfo(r), "mode", "agent_or_server")

			http.Error(w, "authentication failed - no authentication credentials provided", http.StatusUnauthorized)
			return
		}

		if hostname != "" {
			r.Header.Set("X-PBS-Authenticated-Agent", hostname)
		}
		next.ServeHTTP(w, r)
	}))
}

func checkAgentAuth(store *store.Store, r *http.Request) (string, error) {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return "", fmt.Errorf("CheckAgentAuth: client certificate required")
	}

	clientCert := r.TLS.PeerCertificates[0]

	agentHostname := clientCert.Subject.CommonName
	if agentHostname == "" {
		return "", fmt.Errorf("CheckAgentAuth: missing certificate subject common name")
	}

	trustedCert, err := store.Database.LoadAgentHostCert(agentHostname)
	if err != nil {
		return "", fmt.Errorf("CheckAgentAuth: certificate not trusted")
	}

	if !clientCert.Equal(trustedCert) {
		return "", fmt.Errorf("certificate does not match pinned certificate")
	}

	return agentHostname, nil
}

// validateAgentHostnameMatch returns an error if the body hostname doesn't match
func validateAgentHostnameMatch(authHostname, bodyHostname string) error {
	if authHostname == "" {
		return fmt.Errorf("no authenticated agent hostname")
	}
	if bodyHostname == "" {
		return fmt.Errorf("missing hostname in request body")
	}
	if authHostname != bodyHostname {
		return fmt.Errorf("hostname mismatch: authenticated as %q but request claims %q", authHostname, bodyHostname)
	}
	return nil
}

func checkProxyAuth(r *http.Request) error {
	auth, err := GetPBSAuth()
	if err != nil {
		return fmt.Errorf("CheckProxyAuth: failed to get cached PBS auth -> %w", err)
	}

	cookie, err := r.Cookie("__Host-PBSAuthCookie")
	if err != nil {
		cookie, err = r.Cookie("PBSAuthCookie")
		if err != nil {
			return fmt.Errorf("CheckProxyAuth: authentication required -> %w", err)
		}
	}

	valid, err := auth.VerifyTicket(cookie.Value)
	if err != nil {
		return fmt.Errorf("CheckProxyAuth: authentication required -> %w", err)
	}

	if !valid {
		return fmt.Errorf("CheckProxyAuth: auth token invalid -> %s", cookie.Value)
	}

	return nil
}
