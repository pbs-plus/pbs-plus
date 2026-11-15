//go:build linux

// Package s3export serves attached snapshot filesystems read-only over the S3 API, one bucket per share.
package s3export

import (
	"fmt"
	"strings"
)

const defaultRegion = "us-east-1"

type Credential struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type Config struct {
	Region      string       `json:"region,omitempty"`
	TLS         *bool        `json:"tls,omitempty"`
	TLSCertFile string       `json:"tls-cert,omitempty"`
	TLSKeyFile  string       `json:"tls-key,omitempty"`
	Credentials []Credential `json:"credentials"`
}

func (c Config) RegionName() string {
	if c.Region == "" {
		return defaultRegion
	}
	return c.Region
}

func (c Config) TLSEnabled() bool { return c.TLS == nil || *c.TLS }

func (c Config) Validate() error {
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("s3 tls-cert and tls-key must be set together")
	}
	if !c.TLSEnabled() && c.TLSCertFile != "" {
		return fmt.Errorf("s3 tls-cert and tls-key require tls")
	}
	if strings.ContainsAny(c.RegionName(), " /\\\t\r\n") {
		return fmt.Errorf("s3 region %q is invalid", c.RegionName())
	}
	if len(c.Credentials) == 0 {
		return fmt.Errorf("s3 export needs at least one credential")
	}
	seen := make(map[string]struct{}, len(c.Credentials))
	for _, credential := range c.Credentials {
		if len(credential.AccessKey) < 3 || len(credential.AccessKey) > 128 || strings.ContainsAny(credential.AccessKey, " /\\\t\r\n") {
			return fmt.Errorf("s3 access key is invalid")
		}
		if _, dup := seen[credential.AccessKey]; dup {
			return fmt.Errorf("s3 access key %q is configured more than once", credential.AccessKey)
		}
		seen[credential.AccessKey] = struct{}{}
		if len(credential.SecretKey) < 8 {
			return fmt.Errorf("s3 credential %q needs a secret key of at least 8 characters", credential.AccessKey)
		}
	}
	return nil
}

func (c Config) secret(accessKey string) (string, bool) {
	for _, credential := range c.Credentials {
		if credential.AccessKey == accessKey {
			return credential.SecretKey, true
		}
	}
	return "", false
}

// BucketName maps a share name onto a lowercase S3 bucket name; share names are already restricted to [A-Za-z0-9._-].
func BucketName(share string) string {
	return strings.ReplaceAll(strings.ToLower(share), "_", "-")
}
