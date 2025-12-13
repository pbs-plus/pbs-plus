package types

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/utils"
)

var (
	s3BucketRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9\.-]{1,61}[a-z0-9]$`)
)

type Volume struct {
	VolumeName     string `json:"volume_name"`
	TargetName     string `json:"target_name"`
	MetaType       string `json:"meta_type"`
	MetaName       string `json:"meta_name"`
	MetaFS         string `json:"meta_fs"`
	MetaTotalBytes int    `json:"meta_total_bytes,omitempty"`
	MetaUsedBytes  int    `json:"meta_used_bytes,omitempty"`
	MetaFreeBytes  int    `json:"meta_free_bytes,omitempty"`
	MetaTotal      string `json:"meta_total"`
	MetaUsed       string `json:"meta_used"`
	MetaFree       string `json:"meta_free"`
	Accessible     bool   `json:"accessible"`
}

type Target struct {
	Name             string   `json:"name"`
	MountScript      string   `json:"mount_script"`
	AgentVersion     string   `json:"agent_version"`
	ConnectionStatus bool     `json:"connection_status"`
	Auth             string   `json:"-"`
	JobCount         int      `json:"job_count"`
	TokenUsed        string   `json:"token_used"`
	OperatingSystem  string   `json:"os"`
	TargetType       string   `json:"type"`
	AgentHost        string   `json:"host"`
	S3AccessID       string   `json:"s3_access_id"`
	S3Host           string   `json:"s3_host"`
	S3Region         string   `json:"s3_region"`
	S3UseSSL         bool     `json:"s3_ssl"`
	S3UsePathStyle   bool     `json:"s3_path_style"`
	S3Bucket         string   `json:"s3_bucket"`
	S3Secret         string   `json:"-"`
	LocalPath        string   `json:"local_path"`
	Volumes          []Volume `json:"volumes"`
}

func (v Volume) Validate() error {
	if strings.TrimSpace(v.VolumeName) == "" {
		return fmt.Errorf("volume name is empty")
	}
	return nil
}

func (t Target) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return fmt.Errorf("target name is empty")
	}
	switch t.TargetType {
	case "local":
		if strings.TrimSpace(t.LocalPath) == "" {
			return fmt.Errorf("local path is empty")
		}
		if !filepath.IsAbs(t.LocalPath) {
			return fmt.Errorf("local path must be absolute")
		}
		if !utils.IsValid(t.LocalPath) {
			return fmt.Errorf("invalid local path")
		}
	case "agent":
		if strings.TrimSpace(t.AgentHost) == "" {
			return fmt.Errorf("agent host is empty")
		}
		// Volumes optional, but if provided validate each
		for _, v := range t.Volumes {
			if err := v.Validate(); err != nil {
				return fmt.Errorf("volume %q invalid: %w", v.VolumeName, err)
			}
		}
	case "s3":
		if strings.TrimSpace(t.S3AccessID) == "" {
			return fmt.Errorf("s3 access id is empty")
		}
		if strings.TrimSpace(t.S3Host) == "" {
			return fmt.Errorf("s3 host is empty")
		}
		if strings.TrimSpace(t.S3Bucket) == "" {
			return fmt.Errorf("s3 bucket is empty")
		}
		if !s3BucketRegex.MatchString(t.S3Bucket) {
			return fmt.Errorf("invalid s3 bucket %q", t.S3Bucket)
		}
		host := t.S3Host
		if !strings.Contains(host, "://") {
			scheme := "https"
			if !t.S3UseSSL {
				scheme = "http"
			}
			host = scheme + "://" + host
		}
		u, err := url.Parse(host)
		if err != nil || u.Host == "" {
			return fmt.Errorf("invalid s3 host %q", t.S3Host)
		}
	default:
		return fmt.Errorf("invalid target type %q", t.TargetType)
	}

	if t.TargetType != "s3" && (t.S3AccessID != "" || t.S3Bucket != "" || t.S3Host != "") {
		return fmt.Errorf("s3 fields provided for non-s3 target")
	}
	if t.TargetType != "agent" && (t.AgentHost != "" && t.AgentHost != "-") {
		return fmt.Errorf("agent host provided for non-agent target")
	}

	for _, v := range t.Volumes {
		if err := v.Validate(); err != nil {
			return fmt.Errorf("volume %q invalid: %w", v.VolumeName, err)
		}
	}

	return nil
}
