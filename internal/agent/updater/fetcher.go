package updater

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pbs-plus/pbs-plus/internal/agent"
	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

type VersionResp struct {
	Version string `json:"version"`
}

type UpdateFetcher struct {
	currentVersion string
	delay          bool
}

func (u *UpdateFetcher) Init() error {
	if u.currentVersion == "" {
		u.currentVersion = constants.Version
	}
	return nil
}

func (u *UpdateFetcher) Fetch() (io.Reader, error) {
	if u.delay {
		time.Sleep(2 * time.Minute)
	}
	u.delay = true
	newVersion := ""
	if str, err := u.checkForNewVersion(); str == "" || err != nil {
		return nil, nil
	} else {
		newVersion = str
	}

	syslog.L.Info().
		WithMessage("new version found, attempting to auto-update").
		WithField("current", u.currentVersion).
		WithField("new", newVersion).
		Write()

	reader, err := u.downloadUpdate()
	if err == nil && newVersion != "" {
		u.currentVersion = newVersion
	}

	return reader, err
}

func (u *UpdateFetcher) downloadUpdate() (io.Reader, error) {
	expected, err := u.downloadMD5()
	if err != nil || expected == "" {
		return nil, err
	}

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
	if err != nil {
		return nil, err
	}

	h := md5.New()
	if _, err := io.Copy(h, resp); err != nil {
		resp.Close()
		return nil, err
	}
	sum := hex.EncodeToString(h.Sum(nil))
	resp.Close()

	if !strings.EqualFold(sum, strings.TrimSpace(expected)) {
		return nil, errors.New("md5 mismatch")
	}

	return agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary", nil, nil)
}

func (u *UpdateFetcher) checkForNewVersion() (string, error) {
	var versionResp VersionResp

	constraint, err := semver.NewConstraint(">= 0.52.0-rc1")

	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/version", nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Close()

	data, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}

	if err := json.Unmarshal(data, &versionResp); err != nil {
		return "", err
	}

	if versionResp.Version != u.currentVersion {
		vs, err := semver.NewVersion(versionResp.Version)
		if err != nil {
			return "", err
		}
		if !constraint.Check(vs) {
			syslog.L.Info().
				WithMessage("new version does not have new update_fetcher").
				WithField("current", u.currentVersion).
				WithField("new", versionResp.Version).
				Write()
			return "", fmt.Errorf("new version does not have new update_fetcher")
		}
		return versionResp.Version, nil
	}
	return "", nil
}

func (u *UpdateFetcher) downloadMD5() (string, error) {
	resp, err := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/plus/binary/checksum", nil, nil)
	if err != nil {
		return "", err
	}
	defer resp.Close()

	md5Bytes, err := io.ReadAll(resp)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(md5Bytes)), nil
}

