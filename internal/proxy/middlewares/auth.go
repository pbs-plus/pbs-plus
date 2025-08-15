//go:build linux

package middlewares

import (
	"crypto"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type PBSAuth struct {
	privateKey crypto.PrivateKey
	keyType    string
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

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		rsaKey, rsaErr := x509.ParsePKCS1PrivateKey(block.Bytes)
		if rsaErr != nil {
			return nil, fmt.Errorf("failed to parse private key as PKCS#8 or PKCS#1: %v, %v", err, rsaErr)
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
		syslog.L.Error(err).
			WithField("stage", "split").
			WithField("cookie_len", len(cookieVal)).
			WithField("has_percent", strings.Contains(cookieVal, "%")).
			Write()
		return false, fmt.Errorf("invalid ticket format: %w", err)
	}

	if strings.HasPrefix(sigStr, ":") {
		syslog.L.Warn().WithMessage("VerifyTicket: signature had unexpected leading colon").Write()
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
			syslog.L.Error(fmt.Errorf("base64 decode failed: %w", err)).
				WithField("stage", "b64decode").
				WithField("used_decoded_cookie", usedDecoded).
				WithField("sig_len", len(sigStr)).
				WithField("sig_preview", preview(sigStr, 80)).
				WithField("alphabet_hint",
					alphabetHint(sigStr)).
				Write()
			return false, fmt.Errorf("signature base64 decode failed: %w", err)
		}
	}

	switch p.keyType {
	case "ed25519":
		pub := p.privateKey.(ed25519.PrivateKey).Public().(ed25519.PublicKey)
		if !ed25519.Verify(pub, []byte(left), sig) {
			syslog.L.Error(fmt.Errorf("ed25519 signature invalid")).
				WithField("stage", "verify").
				WithField("msg_len", len(left)).
				WithField("msg_prefix", preview(left, 32)).
				WithField("used_decoded_cookie", usedDecoded).
				Write()
			return false, fmt.Errorf("ed25519 signature invalid")
		}
		return true, nil

	case "rsa":
		pub := &p.privateKey.(*rsa.PrivateKey).PublicKey
		sum := sha256.Sum256([]byte(left))
		if err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig); err != nil {
			syslog.L.Error(fmt.Errorf("rsa verify failed: %w", err)).
				WithField("stage", "verify").
				WithField("hash", "sha256").
				WithField("msg_len", len(left)).
				WithField("msg_prefix", preview(left, 32)).
				WithField("used_decoded_cookie", usedDecoded).
				Write()
			return false, fmt.Errorf("rsa signature invalid: %w", err)
		}
		return true, nil

	default:
		syslog.L.Error(fmt.Errorf("unsupported key type: %s", p.keyType)).
			WithField("stage", "verify").
			Write()
		return false, fmt.Errorf("unsupported key type: %s", p.keyType)
	}
}

func splitPBS(raw string) (left, right string, usedDecoded bool, err error) {
	if parts := strings.SplitN(raw, "::", 2); len(parts) == 2 {
		return parts[0], parts[1], false, nil
	}
	if strings.Contains(raw, "%") {
		if parts := strings.SplitN(raw, "%3A%3A", 2); len(parts) == 2 {
			return parts[0], parts[1], false, nil
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
			syslog.L.Error(err).WithField("mode", "server_only").Write()
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
