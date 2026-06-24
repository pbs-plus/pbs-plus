package updater

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/Masterminds/semver"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var (
	ECDSAPublicKeyB64   = ""
	Ed25519PublicKeyB64 = ""
)

type Config struct {
	MinConstraint  string
	PollInterval   time.Duration
	FetchOnStart   bool
	SystemdUnit    string
	UpgradeConfirm func(newVersion string) bool
	Exit           func(error)
	Service        service.Service
	Context        context.Context
}

type Updater struct {
	cfg    Config
	cancel context.CancelFunc
}

type VersionResp struct {
	Version string `json:"version"`
}

func New(cfg Config) (*Updater, error) {
	if cfg.MinConstraint == "" {
		cfg.MinConstraint = ">= 0.52.0"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Minute
	}
	if cfg.UpgradeConfirm == nil {
		cfg.UpgradeConfirm = func(string) bool { return true }
	}
	if cfg.SystemdUnit == "" {
		cfg.SystemdUnit = "pbs-plus-agent"
	}
	if cfg.Exit == nil {
		cfg.Exit = func(err error) {
			if err != nil {
				syslog.L.Error(err).WithMessage("updater exit with error").Write()
			}
			os.Exit(0)
		}
	}

	if err := cleanUp(); err != nil {
		syslog.L.Error(err).WithMessage("update cleanup error, non-fatal").Write()
	}

	ctx, cancel := context.WithCancel(cfg.Context)
	up := &Updater{cfg: cfg, cancel: cancel}

	if cfg.FetchOnStart {
		go func() {
			if err := up.CheckNow(); err != nil {
				syslog.L.Error(err).WithMessage("initial update check failed").Write()
			}
		}()
	}

	if cfg.PollInterval > 0 {
		go up.poll(ctx)
	}

	return up, nil
}

func (u *Updater) poll(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := u.CheckNow(); err != nil {
				syslog.L.Error(err).WithMessage("scheduled update check failed").Write()
			}
		}
	}
}

func (u *Updater) Stop() {
	if u.cancel != nil {
		u.cancel()
	}
}

func (u *Updater) CheckNow() error {
	version, err := u.fetchLatestVersion()
	if err != nil {
		return fmt.Errorf("updater: fetch latest version: %w", err)
	}

	if version == "" || version == conf.Version {
		return fmt.Errorf("updater: already on latest version")
	}

	constr, err := semver.NewConstraint(u.cfg.MinConstraint)
	if err != nil {
		return fmt.Errorf("updater: invalid semver constraint: %w", err)
	}
	vs, err := semver.NewVersion(version)
	if err != nil {
		return fmt.Errorf("updater: parse version %q: %w", version, err)
	}
	if !constr.Check(vs) {
		return fmt.Errorf("updater: version %s does not meet constraint %s", version, u.cfg.MinConstraint)
	}

	if !u.cfg.UpgradeConfirm(version) {
		syslog.L.Info().WithMessage("upgrade not confirmed by user").Write()
		return nil
	}

	pendingVer := version

	if err := u.applyUpdate(pendingVer); err != nil {
		return fmt.Errorf("updater: apply update: %w", err)
	}

	return nil
}

func (u *Updater) fetchLatestVersion() (string, error) {
	resp, err := agent.AgentHTTPRequest(http.MethodGet, "/api2/json/plus/version", nil, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := resp.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	data, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}

	var vr VersionResp
	if err := json.Unmarshal(data, &vr); err != nil {
		return "", fmt.Errorf("unmarshal version response: %w", err)
	}

	return vr.Version, nil
}

func (u *Updater) applyUpdate(version string) error {
	params := fmt.Sprintf("os=%s&arch=%s", runtime.GOOS, runtime.GOARCH)

	binary, err := u.fetchBinary(params)
	if err != nil {
		return fmt.Errorf("fetch binary: %w", err)
	}

	ecdsaSig, ecdsaErr := u.fetchECDSASignature(params)
	ed25519Sig, ed25519Err := u.fetchEd25519Signature(params)

	verified := false

	if ecdsaSig != nil {
		if err := verifyWithECDSA(binary, ecdsaSig); err != nil {
			syslog.L.Error(err).WithMessage("ECDSA P-256 signature verification failed").Write()
		} else {
			verified = true
			syslog.L.Info().WithMessage("update verified with ECDSA P-256 signature (FIPS-approved)").Write()
		}
	}

	if !verified && ed25519Sig != nil {
		if err := verifyWithEd25519(binary, ed25519Sig); err != nil {
			syslog.L.Error(err).WithMessage("Ed25519 signature verification failed").Write()
		} else {
			verified = true
			syslog.L.Warn().WithMessage("update verified with Ed25519 signature (non-FIPS, legacy fallback; will be removed in future release)").Write()
		}
	}

	if !verified {
		if ecdsaErr != nil {
			syslog.L.Error(ecdsaErr).WithMessage("failed to fetch ECDSA signature").Write()
		}
		if ed25519Err != nil {
			syslog.L.Error(ed25519Err).WithMessage("failed to fetch Ed25519 signature").Write()
		}
		return fmt.Errorf("no valid signature found for update")
	}

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}

	newPath := exePath + ".new"
	oldPath := exePath + ".old"

	if err := os.WriteFile(newPath, binary, 0755); err != nil {
		return fmt.Errorf("write new binary: %w", err)
	}

	if err := os.Rename(exePath, oldPath); err != nil {
		return fmt.Errorf("rename current binary: %w", err)
	}

	if err := os.Rename(newPath, exePath); err != nil {
		rerr := os.Rename(oldPath, exePath)
		if rerr != nil {
			syslog.L.Error(rerr).WithMessage("rollback failed: could not restore original binary").Write()
		}
		return fmt.Errorf("rename new binary: %w", err)
	}

	_ = os.Remove(oldPath)

	restartCallback(u.cfg)
	return nil
}

func (u *Updater) fetchBinary(params string) ([]byte, error) {
	url := fmt.Sprintf("/api2/json/plus/binary?%s", params)
	rc, err := agent.AgentHTTPRequest(http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch binary: %w", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read binary: %w", err)
	}
	return data, nil
}

func (u *Updater) fetchECDSASignature(params string) ([]byte, error) {
	url := fmt.Sprintf("/api2/json/plus/binary/ecdsa-sig?%s", params)
	rc, err := agent.AgentHTTPRequest(http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch ECDSA signature: %w", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read ECDSA signature: %w", err)
	}
	return data, nil
}

func (u *Updater) fetchEd25519Signature(params string) ([]byte, error) {
	url := fmt.Sprintf("/api2/json/plus/binary/sig?%s", params)
	rc, err := agent.AgentHTTPRequest(http.MethodGet, url, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch Ed25519 signature: %w", err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read Ed25519 signature: %w", err)
	}
	return data, nil
}

func loadECDSAPublicKey() (*ecdsa.PublicKey, error) {
	if ECDSAPublicKeyB64 == "" {
		return nil, fmt.Errorf("ECDSA public key not configured")
	}

	derData, err := base64.StdEncoding.DecodeString(ECDSAPublicKeyB64)
	if err != nil {
		return nil, fmt.Errorf("decode ECDSA public key: %w", err)
	}

	pub, err := x509.ParsePKIXPublicKey(derData)
	if err != nil {
		return nil, fmt.Errorf("parse ECDSA public key: %w", err)
	}

	ecdsaPub, ok := pub.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is not ECDSA (got %T)", pub)
	}

	if ecdsaPub.Curve != elliptic.P256() {
		return nil, fmt.Errorf("ECDSA public key must be P-256, got curve %s", ecdsaPub.Curve.Params().Name)
	}

	return ecdsaPub, nil
}

func loadEd25519PublicKey() (ed25519.PublicKey, error) {
	if Ed25519PublicKeyB64 == "" {
		return nil, fmt.Errorf("Ed25519 public key not configured")
	}

	b, err := base64.StdEncoding.DecodeString(Ed25519PublicKeyB64)
	if err != nil {
		return nil, fmt.Errorf("decode Ed25519 public key: %w", err)
	}

	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid Ed25519 public key size: got %d, want %d", len(b), ed25519.PublicKeySize)
	}

	return ed25519.PublicKey(b), nil
}

func verifyWithECDSA(binary, sigData []byte) error {
	pubKey, err := loadECDSAPublicKey()
	if err != nil {
		return fmt.Errorf("load ECDSA public key: %w", err)
	}

	sig, err := parseECDSASignature(sigData)
	if err != nil {
		return fmt.Errorf("parse ECDSA signature: %w", err)
	}

	hash := sha256.Sum256(binary)
	if !ecdsa.Verify(pubKey, hash[:], sig.R, sig.S) {
		return fmt.Errorf("ECDSA P-256 signature verification failed")
	}

	return nil
}

func verifyWithEd25519(binary, sigData []byte) error {
	pubKey, err := loadEd25519PublicKey()
	if err != nil {
		return fmt.Errorf("load Ed25519 public key: %w", err)
	}

	if len(sigData) != ed25519.SignatureSize {
		return fmt.Errorf("invalid Ed25519 signature length: got %d, want %d", len(sigData), ed25519.SignatureSize)
	}

	if !ed25519.Verify(pubKey, binary, sigData) {
		return fmt.Errorf("Ed25519 signature verification failed")
	}

	return nil
}

type ecdsaSig struct {
	R, S *big.Int
}

func parseECDSASignature(data []byte) (*ecdsaSig, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("base64 decode signature: %w", err)
	}

	var sig ecdsaSig
	if _, err := asn1.Unmarshal(decoded, &sig); err != nil {
		return nil, fmt.Errorf("ASN.1 unmarshal signature: %w", err)
	}

	return &sig, nil
}
