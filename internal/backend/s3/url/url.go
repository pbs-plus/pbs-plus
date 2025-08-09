package s3url

import (
	"fmt"
	"net/url"
	"strings"
)

// S3Url represents a parsed S3-compatible URL
type S3Url struct {
	Scheme    string // https, http, s3
	UseSSL    bool   // true if https, false if http
	Endpoint  string // s3.amazonaws.com, minio.example.com, etc.
	Region    string // optional
	AccessKey string // optional (from user:pass@host)
	Bucket    string
	Key       string // object key / prefix
}

// Parse parses an S3-compatible URL into an S3Url struct
func Parse(raw string) (*S3Url, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	s3 := &S3Url{
		Scheme: strings.ToLower(u.Scheme),
		UseSSL: strings.ToLower(u.Scheme) == "https",
	}

	// Extract access key if present in userinfo
	if u.User != nil {
		s3.AccessKey = u.User.Username()
	}

	hostParts := strings.Split(u.Host, ".")
	pathParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")

	// Detect virtual-host-style vs path-style
	if len(hostParts) > 2 {
		// virtual-host-style: bucket.endpoint
		s3.Bucket = hostParts[0]
		s3.Endpoint = strings.Join(hostParts[1:], ".")
		if len(pathParts) > 0 && pathParts[0] != "" {
			s3.Key = strings.Join(pathParts, "/")
		}
	} else {
		// path-style: endpoint/bucket/key
		s3.Endpoint = u.Host
		if len(pathParts) > 0 && pathParts[0] != "" {
			s3.Bucket = pathParts[0]
		}
		if len(pathParts) > 1 {
			s3.Key = strings.Join(pathParts[1:], "/")
		}
	}

	// Try to detect region from endpoint (optional)
	if strings.Contains(s3.Endpoint, "s3.") {
		parts := strings.Split(s3.Endpoint, ".")
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "s3" && i+1 < len(parts) {
				s3.Region = parts[i+1]
				break
			}
		}
	}

	return s3, nil
}

// Format builds a URL string from an S3Url struct
func (s *S3Url) Format(virtualHost bool) string {
	var host string
	if virtualHost {
		host = fmt.Sprintf("%s.%s", s.Bucket, s.Endpoint)
	} else {
		host = s.Endpoint
	}

	var path string
	if virtualHost {
		path = s.Key
	} else {
		path = fmt.Sprintf("%s/%s", s.Bucket, s.Key)
	}

	scheme := s.Scheme
	if scheme == "" {
		if s.UseSSL {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}

	u := url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   path,
	}

	if s.AccessKey != "" {
		u.User = url.User(s.AccessKey)
	}

	return u.String()
}
