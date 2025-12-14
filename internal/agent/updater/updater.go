package updater

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/fynelabs/selfupdate"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type Config struct {
	MinConstraint  string
	PollInterval   time.Duration
	FetchOnStart   bool
	SystemdUnit    string
	UpgradeConfirm func(newVersion string) bool
	Exit           func(error)
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
		if strings.Contains(err.Error(), "on the latest version") {
			syslog.L.Debug().WithMessage(err.Error()).WithField("source", "selfupdate").Write()
			return
		}

		syslog.L.Error(err).WithField("source", "selfupdate").Write()
	}
	selfupdate.LogInfo = func(format string, args ...any) {
		info := fmt.Sprintf(format, args...)
		if strings.Contains(info, "Scheduled upgrade check after") {
			syslog.L.Debug().WithMessage(info).WithField("source", "selfupdate").Write()
			return
		}

		syslog.L.Info().WithMessage(fmt.Sprintf(format, args...)).WithField("source", "selfupdate").Write()
	}
	selfupdate.LogDebug = func(format string, args ...any) {
		syslog.L.Debug().WithMessage(fmt.Sprintf(format, args...)).WithField("source", "selfupdate").Write()
	}

	src := &agentSource{
		versionURL: "/api2/json/plus/version",
		binaryURL:  "/api2/json/plus/binary",
		md5URL:     "/api2/json/plus/binary/checksum",
		currentVer: constants.Version,
		minConstr:  cfg.MinConstraint,
	}

	up := &Updater{cfg: cfg}
	manager, err := selfupdate.Manage(&selfupdate.Config{
		Source:   src,
		Schedule: selfupdate.Schedule{FetchOnStart: cfg.FetchOnStart, Interval: cfg.PollInterval},
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
			restartCallback(cfg)
			return false
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
	md5URL         string
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
	expectedHex, err := a.fetchMD5()
	if err != nil || expectedHex == "" {
		if err == nil {
			err = fmt.Errorf("empty checksum")
		}
		return nil, 0, fmt.Errorf("md5 fetch failed: %w", err)
	}
	expected, err := hex.DecodeString(strings.TrimSpace(expectedHex))
	if err != nil {
		return nil, 0, fmt.Errorf("invalid md5 hex: %w", err)
	}

	rc, err := agent.AgentHTTPRequest(http.MethodGet, a.binaryURL, nil, nil)
	if err != nil {
		return nil, 0, err
	}

	wrapped := newMD5VerifyingReadCloser(rc, expected)
	return wrapped, -1, nil
}

func (a *agentSource) GetSignature() ([64]byte, error) {
	var sig [64]byte
	for i := 0; i < len(sig); i++ {
		sig[i] = byte(i)
	}
	return sig, nil
}

func (a *agentSource) fetchMD5() (string, error) {
	rc, err := agent.AgentHTTPRequest(http.MethodGet, a.md5URL, nil, nil)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

type md5VerifyingReadCloser struct {
	src      io.ReadCloser
	h        hashWriter
	expected []byte
	done     bool
}

type hashWriter struct {
	h io.Writer
	n interface {
		Sum([]byte) []byte
		Write([]byte) (int, error)
	}
}

func newMD5VerifyingReadCloser(src io.ReadCloser, expected []byte) io.ReadCloser {
	return &md5VerifyingReadCloser{
		src:      src,
		h:        newMD5(),
		expected: expected,
	}
}

func newMD5() hashWriter {
	h := md5.New()
	return hashWriter{h: h, n: h}
}

func (r *md5VerifyingReadCloser) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	if n > 0 {
		_, _ = r.h.n.Write(p[:n])
	}
	if err == io.EOF && !r.done {
		sum := r.h.n.Sum(nil)
		if !bytes.Equal(sum, r.expected) {
			return n, fmt.Errorf("md5 mismatch")
		}
		r.done = true
	}
	return n, err
}

func (r *md5VerifyingReadCloser) Close() error {
	return r.src.Close()
}
