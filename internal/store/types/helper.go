//go:build linux

package types

import (
	"database/sql/driver"
	"fmt"
	"strings"

	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
)

func NewTargetPath(targetType TargetType, data string, volume string, os string) TargetPath {
	var raw string
	switch targetType {
	case TargetTypeAgent:
		if os == "windows" {
			raw = "agent://" + data + "/" + volume
		} else {
			raw = "agent://" + data + "/root"
		}
	default:
		raw = data
	}

	tp := TargetPath{Raw: raw}
	info := tp.parsePathInfo()
	tp.info = info
	return tp
}

func NewTargetName(targetType TargetType, data string, volume string, os string) TargetName {
	var raw string
	var host, vol string

	if targetType == TargetTypeAgent {
		if os == "windows" {
			raw = data + " - " + volume
			host = data
			vol = volume
		} else {
			raw = data + " - Root"
			host = data
			vol = "Root"
		}
	} else {
		raw = data
		host = data
	}

	return TargetName{
		Raw:      raw,
		hostname: host,
		volume:   vol,
	}
}

func WrapTargetPath(raw string) TargetPath {
	return TargetPath{
		Raw: raw,
	}
}

func WrapTargetName(raw string) TargetName {
	return TargetName{
		Raw: raw,
	}
}

func (n *TargetName) hydrate() {
	if i := strings.Index(n.Raw, " - "); i != -1 {
		n.hostname = n.Raw[:i]
		n.volume = n.Raw[i+3:]
	} else {
		n.hostname = n.Raw
	}
}

func (n TargetName) GetHostname() string {
	if n.Raw == "" {
		return ""
	}
	if n.hostname != "" {
		return n.hostname
	}

	n.hydrate()
	return n.hostname
}

func (n TargetName) GetVolume() string {
	if n.Raw == "" {
		return ""
	}
	if n.volume != "" {
		return n.volume
	}

	n.hydrate()
	return n.volume
}

func (t *TargetPath) hydrate() {
	t.info = t.parsePathInfo()
}

func (t TargetPath) GetPathInfo() PathInfo {
	if t.Raw == "" {
		t.info = nil
		return PathInfo{}
	}
	if t.info != nil {
		return *t.info
	}

	t.hydrate()
	return *t.info
}

func (t TargetPath) IsAgent() bool {
	return strings.HasPrefix(t.Raw, "agent://")
}

func (t TargetPath) IsS3() bool {
	return t.GetPathInfo().Type == TargetTypeS3
}

func (t TargetPath) IsLocal() bool {
	return t.GetPathInfo().Type == TargetTypeLocal
}

func (t TargetPath) parsePathInfo() *PathInfo {
	isWindows := false
	if strings.HasPrefix(t.Raw, "agent://") {
		trimmed := t.Raw[8:]
		lastSlash := strings.LastIndex(trimmed, "/")
		hostPath := ""
		if lastSlash != -1 {
			res := trimmed[lastSlash+1:]
			switch {
			case res == "root":
				hostPath = "/"
			case len(res) == 1:
				hostPath = res + ":\\"
				isWindows = true
			default:
				hostPath = res
			}
		}
		return &PathInfo{Type: TargetTypeAgent, RawPath: t.Raw, HostPath: hostPath, IsWindows: isWindows}
	}

	if strings.Contains(t.Raw, "://") {
		if s3, err := s3url.Parse(t.Raw); err == nil {
			return &PathInfo{Type: TargetTypeS3, RawPath: t.Raw, S3Url: s3}
		}
	}

	return &PathInfo{Type: TargetTypeLocal, RawPath: t.Raw}
}

func (t TargetPath) String() string { return t.Raw }
func (n TargetName) String() string { return n.Raw }

func (t TargetPath) MarshalText() ([]byte, error) {
	return []byte(t.Raw), nil
}

func (t *TargetPath) UnmarshalText(text []byte) error {
	t.Raw = string(text)
	info := t.parsePathInfo()
	t.info = info
	return nil
}

func (t TargetName) MarshalText() ([]byte, error) {
	return []byte(t.Raw), nil
}

func (t *TargetName) UnmarshalText(text []byte) error {
	s := string(text)
	t.Raw = s
	if i := strings.Index(s, " - "); i != -1 {
		t.hostname = s[:i]
		t.volume = s[i+3:]
	} else {
		t.hostname = s
	}
	return nil
}

func (t TargetPath) Value() (driver.Value, error) {
	return t.Raw, nil
}

func (n TargetName) Value() (driver.Value, error) {
	return n.Raw, nil
}

func (t *TargetPath) Scan(value any) error {
	if value == nil {
		t.Raw = ""
		return nil
	}
	s, ok := value.(string)
	if !ok {
		if b, ok := value.([]byte); ok {
			s = string(b)
		} else {
			return fmt.Errorf("failed to scan TargetPath: %v", value)
		}
	}
	t.Raw = s
	return nil
}

func (n *TargetName) Scan(value any) error {
	if value == nil {
		n.Raw = ""
		return nil
	}
	s, ok := value.(string)
	if !ok {
		if b, ok := value.([]byte); ok {
			s = string(b)
		} else {
			return fmt.Errorf("failed to scan TargetName: %v", value)
		}
	}
	n.Raw = s
	return nil
}

func (j *Backup) GetStreamID() string {
	if j.TargetPath.IsLocal() {
		return ""
	}

	if j.TargetPath.IsS3() {
		return j.TargetPath.GetPathInfo().S3Url.Endpoint + "|" + j.ID
	}

	return j.Target.GetHostname() + "|" + j.ID
}

func (j *Restore) GetStreamID() string {
	if j.DestTargetPath.IsLocal() {
		return ""
	}

	if j.DestTargetPath.IsS3() {
		return j.DestTargetPath.GetPathInfo().S3Url.Endpoint + "|" + j.ID + "|restore"
	}

	return j.DestTarget.GetHostname() + "|" + j.ID + "|restore"
}

