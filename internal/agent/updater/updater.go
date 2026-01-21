package updater

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/fynelabs/selfupdate"
	"github.com/kardianos/service"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

var Ed25519PublicKeyB64 = ""

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
	cfg     Config
	manager *selfupdate.Updater
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

	selfupdate.LogError = func(format string, args ...any) {
		err := fmt.Errorf(format, args...)
		lineErrs := strings.SplitSeq(err.Error(), "\n")
		for err := range lineErrs {
			if strings.TrimSpace(err) == "" {
				continue
			}
			if strings.Contains(err, "on the latest version") {
				syslog.L.Debug().WithMessage(err).WithField("source", "selfupdate").Write()
				return
			}

			syslog.L.Error(errors.New(err)).WithField("source", "selfupdate").Write()
		}
	}
	selfupdate.LogInfo = func(format string, args ...any) {
		info := fmt.Sprintf(format, args...)
		lines := strings.SplitSeq(info, "\n")
		for line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			if strings.Contains(line, "Scheduled upgrade check after") {
				syslog.L.Debug().WithMessage(line).WithField("source", "selfupdate").Write()
				return
			}

			syslog.L.Info().WithMessage(line).WithField("source", "selfupdate").Write()
		}
	}
	selfupdate.LogDebug = func(format string, args ...any) {
		lines := strings.SplitSeq(fmt.Sprintf(format, args...), "\n")
		for line := range lines {
			if strings.TrimSpace(line) == "" {
				continue
			}
			syslog.L.Debug().WithMessage(line).WithField("source", "selfupdate").Write()
		}
	}

	params := url.Values{}
	params.Add("os", runtime.GOOS)
	params.Add("arch", runtime.GOARCH)
	queryString := params.Encode()

	src := &agentSource{
		versionURL: "/api2/json/plus/version",
		binaryURL:  fmt.Sprintf("/api2/json/plus/binary?%s", queryString),
		sigURL:     fmt.Sprintf("/api2/json/plus/binary/sig?%s", queryString),
		currentVer: constants.Version,
		minConstr:  cfg.MinConstraint,
	}

	pubKey, err := loadEd25519Pub()
	if err != nil {
		return nil, err
	}

	up := &Updater{cfg: cfg}
	manager, err := selfupdate.Manage(&selfupdate.Config{
		Source:    src,
		PublicKey: pubKey,
		Schedule:  selfupdate.Schedule{FetchOnStart: cfg.FetchOnStart, Interval: cfg.PollInterval},
		ProgressCallback: func(f float64, err error) {
			if err != nil {
				syslog.L.Error(err).WithMessage("update download error").Write()
				return
			}
			if f >= 0 {
				syslog.L.Debug().WithMessage("update downloading").WithField("progress", fmt.Sprintf("%.2f", f)).Write()
			} else {
				syslog.L.Debug().WithMessage("update downloading (size unknown)").Write()
			}
		},
		UpgradeConfirmCallback: func(_ string) bool {
			return cfg.UpgradeConfirm(src.pendingVersion)
		},
		RestartConfirmCallback: func() bool {
			return restartCallback(cfg)
		},
		ExitCallback: func(err error) {
			cfg.Exit(err)
		},
	})
	if err != nil {
		return nil, err
	}

	// remove old updater if it exists
	err = cleanUp()
	if err != nil {
		syslog.L.Error(err).WithMessage("update cleanup error, non-fatal").Write()
	}

	up.manager = manager
	return up, nil
}

func (u *Updater) CheckNow() error {
	if u.manager == nil {
		return fmt.Errorf("updater not initialized")
	}
	return u.manager.CheckNow()
}

func (u *Updater) Restart() error {
	if u.manager == nil {
		return fmt.Errorf("updater not initialized")
	}
	return u.manager.Restart()
}

type agentSource struct {
	versionURL     string
	binaryURL      string
	sigURL         string
	currentVer     string
	minConstr      string
	pendingVersion string
}

type VersionResp struct {
	Version string `json:"version"`
}

func (a *agentSource) LatestVersion() (*selfupdate.Version, error) {
	resp, err := agent.AgentHTTPRequest(http.MethodGet, a.versionURL, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Close()

	var vr VersionResp
	data, err := io.ReadAll(resp)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &vr); err != nil {
		return nil, err
	}
	if vr.Version == "" || vr.Version == a.currentVer {
		return nil, fmt.Errorf("current binary is on the latest version")
	}

	constr, err := semver.NewConstraint(a.minConstr)
	if err != nil {
		return nil, fmt.Errorf("invalid semver constraint: %w", err)
	}
	vs, err := semver.NewVersion(vr.Version)
	if err != nil {
		return nil, err
	}
	if !constr.Check(vs) {
		return nil, fmt.Errorf("available version does not meet updater constraint")
	}

	a.pendingVersion = vr.Version
	return &selfupdate.Version{
		Number: vr.Version,
		Date:   time.Now(),
	}, nil
}

func (a *agentSource) Get(v *selfupdate.Version) (io.ReadCloser, int64, error) {
	rc, err := agent.AgentHTTPRequest(http.MethodGet, a.binaryURL, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	return rc, -1, nil
}

func loadEd25519Pub() (ed25519.PublicKey, error) {
	b, err := base64.StdEncoding.DecodeString(Ed25519PublicKeyB64)
	if err != nil {
		return nil, fmt.Errorf("invalid embedded public key: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key size: %d", len(b))
	}
	return ed25519.PublicKey(b), nil
}

func (a *agentSource) GetSignature() ([64]byte, error) {
	var sig64 [64]byte
	rc, err := agent.AgentHTTPRequest(http.MethodGet, a.sigURL, nil, nil)
	if err != nil {
		return sig64, err
	}
	defer rc.Close()
	sigBytes, err := io.ReadAll(rc)
	if err != nil {
		return sig64, err
	}
	if len(sigBytes) != 64 {
		return sig64, fmt.Errorf("invalid signature length: got %d, want 64", len(sigBytes))
	}
	copy(sig64[:], sigBytes)
	return sig64, nil
}
