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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/Masterminds/semver"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

var (
	ECDSAPublicKeyB64   = ""
	Ed25519PublicKeyB64 = ""

	ErrAlreadyLatest = errors.New("already on latest version")

	defaultMu      sync.Mutex
	defaultUpdater *Updater
)

const (
	maxBinarySize  = 200 << 20
	maxSigSize     = 4 << 10
	maxVersionSize = 1 << 10
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
	mu     sync.Mutex
}

type VersionResp struct {
	Version  string `json:"version"`
	Embedded bool   `json:"embedded,omitempty"`
}

func New(cfg Config) (*Updater, error) {
	if cfg.MinConstraint == "" {
		cfg.MinConstraint = ">= 0.52.0"
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
				log.Error(err, "updater exit with error")
			}
			os.Exit(0)
		}
	}

	if err := cleanUp(); err != nil {
		log.Error(err, "update cleanup error, non-fatal")
	}

	pruneStaleArtifacts()

	ctx, cancel := context.WithCancel(cfg.Context)
	up := &Updater{cfg: cfg, cancel: cancel}

	defaultMu.Lock()
	defaultUpdater = up
	defaultMu.Unlock()

	if cfg.FetchOnStart {
		go func() {
			if err := up.CheckNow(); err != nil && !errors.Is(err, ErrAlreadyLatest) {
				log.Error(err, "initial update check failed")
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
			if err := u.CheckNow(); err != nil && !errors.Is(err, ErrAlreadyLatest) {
				log.Error(err, "scheduled update check failed")
			}
		}
	}
}

func (u *Updater) Stop() {
	if u.cancel != nil {
		u.cancel()
	}
}

// TriggerUpdate performs a single, on-demand update check using the default
// updater if one was registered via New. This is invoked by the push-based
// update path (aRPC "update" method) so the server can request an update from
// the UI. It is safe to call concurrently with the poll loop.
func TriggerUpdate() error {
	defaultMu.Lock()
	up := defaultUpdater
	defaultMu.Unlock()

	if up != nil {
		return up.CheckNow()
	}

	// No default updater registered (e.g. auto-update disabled). Build a
	// one-shot updater that reuses the same check/apply logic without
	// starting any background goroutines.
	up = &Updater{cfg: Config{
		MinConstraint:  ">= 0.52.0",
		UpgradeConfirm: func(string) bool { return true },
		Exit: func(err error) {
			if err != nil {
				log.Error(err, "updater exit with error")
			}
			os.Exit(0)
		},
	}}
	return up.CheckNow()
}

func (u *Updater) CheckNow() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if exePath, err := os.Executable(); err == nil {
		if p, ok := readPending(exePath); ok {
			return fmt.Errorf("updater: update to %s still pending health confirmation (attempt %d)", p.Version, p.Attempts)
		}
	}

	version, embedded, err := u.fetchLatestVersion()
	if err != nil {
		return fmt.Errorf("updater: fetch latest version: %w", err)
	}

	if version == "" || version == conf.Version {
		return fmt.Errorf("%w: server=%q current=%q", ErrAlreadyLatest, version, conf.Version)
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

	currentVer, cverErr := semver.NewVersion(conf.Version)
	if cverErr == nil && !vs.GreaterThan(currentVer) {
		return fmt.Errorf("updater: version %s is not newer than current version %s", version, conf.Version)
	}

	if !u.cfg.UpgradeConfirm(version) {
		return nil
	}

	if err := u.applyUpdate(version, embedded); err != nil {
		return fmt.Errorf("updater: apply update: %w", err)
	}

	return nil
}

func (u *Updater) fetchLatestVersion() (string, bool, error) {
	resp, err := agent.AgentHTTPRequest(http.MethodGet, "/api2/json/plus/version", nil, nil)
	if err != nil {
		return "", false, err
	}
	defer func() {
		if err := resp.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	data, err := io.ReadAll(io.LimitReader(resp, maxVersionSize))
	if err != nil {
		return "", false, err
	}
	if len(data) >= maxVersionSize {
		return "", false, fmt.Errorf("version response exceeds maximum size of %d bytes", maxVersionSize)
	}

	var vr VersionResp
	if err := json.Unmarshal(data, &vr); err != nil {
		return "", false, fmt.Errorf("unmarshal version response: %w", err)
	}

	return vr.Version, vr.Embedded, nil
}

func (u *Updater) applyUpdate(version string, embedded bool) error {
	params := fmt.Sprintf("os=%s&arch=%s", runtime.GOOS, runtime.GOARCH)

	binary, err := u.fetchBinary(params)
	if err != nil {
		return fmt.Errorf("fetch binary: %w", err)
	}

	if err := sanityCheckBinary(binary); err != nil {
		return fmt.Errorf("sanity check: %w", err)
	}

	verified := false

	if embedded {
		verified = true
		log.Info("update verified via embedded server binary (no external signature required)")
	}

	if !verified {
		ecdsaSig, ecdsaErr := u.fetchECDSASignature(params)
		ed25519Sig, ed25519Err := u.fetchEd25519Signature(params)

		if ecdsaSig != nil {
			if err := verifyWithECDSA(binary, ecdsaSig); err != nil {
				log.Error(err, "ECDSA P-256 signature verification failed")
			} else {
				verified = true
				log.Info("update verified with ECDSA P-256 signature (FIPS-approved)")
			}
		}

		if !verified && ed25519Sig != nil {
			if err := verifyWithEd25519(binary, ed25519Sig); err != nil {
				log.Error(err, "Ed25519 signature verification failed")
			} else {
				verified = true
				log.Warn("update verified with Ed25519 signature (non-FIPS, legacy fallback; will be removed in future release)")
			}
		}

		if !verified {
			if ecdsaErr != nil {
				log.Error(ecdsaErr, "failed to fetch ECDSA signature")
			}
			if ed25519Err != nil {
				log.Error(ed25519Err, "failed to fetch Ed25519 signature")
			}
			return fmt.Errorf("no valid signature found for update")
		}
	}

	exePath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("get executable path: %w", err)
	}

	staged := stagedPathOf(exePath)
	previous := previousPathOf(exePath)

	if err := atomicWriteFile(staged, binary, 0o755); err != nil {
		return fmt.Errorf("stage new binary: %w", err)
	}

	if err := copyFile(previous, exePath, 0o755); err != nil {
		_ = os.Remove(staged)
		return fmt.Errorf("snapshot previous binary: %w", err)
	}

	pending := &pendingUpdate{
		Version:      version,
		PreviousPath: previous,
		StartedAt:    time.Now(),
	}
	if err := writePending(exePath, pending); err != nil {
		_ = os.Remove(staged)
		return fmt.Errorf("write pending marker: %w", err)
	}

	if err := swapBinary(staged, exePath); err != nil {
		removePending(exePath)
		return fmt.Errorf("swap binary: %w", err)
	}

	log.Info("updater: staged update applied, awaiting health confirmation",
		"version", version)

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
			log.Error(err, "")
		}
	}()

	data, err := io.ReadAll(io.LimitReader(rc, maxBinarySize+1))
	if err != nil {
		return nil, fmt.Errorf("read binary: %w", err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("binary download is empty (0 bytes)")
	}
	if len(data) > maxBinarySize {
		return nil, fmt.Errorf("binary exceeds maximum allowed size of %d bytes", maxBinarySize)
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
			log.Error(err, "")
		}
	}()

	data, err := io.ReadAll(io.LimitReader(rc, maxSigSize+1))
	if err != nil {
		return nil, fmt.Errorf("read ECDSA signature: %w", err)
	}
	if len(data) > maxSigSize {
		return nil, fmt.Errorf("ECDSA signature exceeds maximum allowed size of %d bytes", maxSigSize)
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
			log.Error(err, "")
		}
	}()

	data, err := io.ReadAll(io.LimitReader(rc, maxSigSize+1))
	if err != nil {
		return nil, fmt.Errorf("read Ed25519 signature: %w", err)
	}
	if len(data) > maxSigSize {
		return nil, fmt.Errorf("Ed25519 signature exceeds maximum allowed size of %d bytes", maxSigSize)
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

func sanityCheckBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("binary too small (%d bytes)", len(data))
	}
	switch runtime.GOOS {
	case "windows":
		if data[0] != 'M' || data[1] != 'Z' {
			return fmt.Errorf("missing PE (MZ) magic")
		}
	case "darwin":
		magic := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
		switch magic {
		case 0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe, 0xcafebabe:
		default:
			return fmt.Errorf("missing Mach-O magic")
		}
	default:
		if data[0] != 0x7f || data[1] != 'E' || data[2] != 'L' || data[3] != 'F' {
			return fmt.Errorf("missing ELF magic")
		}
	}
	return nil
}

func swapBinary(staged, exePath string) error {
	tmpOld := exePath + ".swapping"
	_ = os.Remove(tmpOld)

	if err := os.Rename(exePath, tmpOld); err != nil {
		return fmt.Errorf("move current binary aside: %w", err)
	}
	if err := os.Rename(staged, exePath); err != nil {
		if rerr := os.Rename(tmpOld, exePath); rerr != nil {
			log.Error(rerr, "updater: swap rollback failed, original binary could not be restored")
		}
		return fmt.Errorf("install staged binary: %w", err)
	}
	_ = os.Remove(tmpOld)
	return nil
}
