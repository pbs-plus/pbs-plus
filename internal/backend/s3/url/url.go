package s3url

import (
	"fmt"
	"net/url"
	"strings"
)

// S3Url represents a parsed S3-compatible URL
type S3Url struct {
	Scheme      string // https, http, s3
	UseSSL      bool   // true if https, false if http
	Endpoint    string // s3.amazonaws.com, minio.example.com, etc.
	Region      string // optional
	AccessKey   string // optional (from user:pass@host)
	Bucket      string
	Key         string // object key / prefix
	IsPathStyle bool
}

// Parse parses an S3-compatible URL into an S3Url struct
func Parse(raw string) (*S3Url, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Must have a scheme
	if u.Scheme == "" {
		return nil, fmt.Errorf("invalid S3 URL: missing scheme (got %q)", raw)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid S3 URL: invalid scheme (got %q)", u.Scheme)
	}

	// Must have a host
	if u.Host == "" {
		return nil, fmt.Errorf("invalid S3 URL: missing host/endpoint (got %q)", raw)
	}

	s3 := &S3Url{
		Scheme: strings.ToLower(u.Scheme),
		UseSSL: strings.ToLower(u.Scheme) == "https",
	}

	// Extract access key if present in userinfo
	if u.User != nil {
		s3.AccessKey = u.User.Username()
	}

	pathParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")

	// Check if this is virtual-host-style by looking for bucket in hostname
	// Virtual-host-style: bucket.s3.amazonaws.com or bucket.endpoint
	// Path-style: endpoint/bucket/key

	isVirtualHost := false
	var bucketFromHost string

	// Check if host starts with a potential bucket name followed by a dot
	// and the remaining part looks like an S3 endpoint
	if dotIndex := strings.Index(u.Host, "."); dotIndex > 0 {
		potentialBucket := u.Host[:dotIndex]
		remainingHost := u.Host[dotIndex+1:]

		// If the path is empty or starts with an object key (not a bucket name),
		// and the host has the pattern bucket.endpoint, treat as virtual-host
		if len(pathParts) == 0 || (len(pathParts) == 1 && pathParts[0] == "") {
			// No path, so bucket must be in hostname
			isVirtualHost = true
			bucketFromHost = potentialBucket
			s3.Endpoint = remainingHost
		} else {
			// Check if this looks like virtual-host by seeing if the first path
			// segment could be an object key rather than a bucket
			// This is a heuristic - in ambiguous cases, prefer path-style

			// Common S3 endpoint patterns that suggest virtual-host style
			if strings.Contains(remainingHost, "amazonaws.com") ||
				strings.Contains(remainingHost, "s3.") ||
				strings.HasPrefix(remainingHost, "s3-") {
				isVirtualHost = true
				bucketFromHost = potentialBucket
				s3.Endpoint = remainingHost
			}
		}
	}

	if isVirtualHost {
		// Virtual-host-style: bucket.endpoint/key
		s3.Bucket = bucketFromHost
		if len(pathParts) > 0 && pathParts[0] != "" {
			s3.Key = strings.Join(pathParts, "/")
		}
	} else {
		// Path-style: endpoint/bucket/key
		s3.Endpoint = u.Host
		if len(pathParts) > 0 && pathParts[0] != "" {
			s3.Bucket = pathParts[0]
		}
		if len(pathParts) > 1 {
			s3.Key = strings.Join(pathParts[1:], "/")
		}
	}

	// Must have a bucket
	if s3.Bucket == "" {
		return nil, fmt.Errorf("invalid S3 URL: missing bucket (got %q)", raw)
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

	s3.IsPathStyle = !isVirtualHost

	return s3, nil
}

// Format builds a URL string from an S3Url struct
func (s *S3Url) Format(virtualHost bool) string {
	var host string
	var path string

	if virtualHost {
		host = fmt.Sprintf("%s.%s", s.Bucket, s.Endpoint)
		path = s.Key
	} else {
		host = s.Endpoint
		if s.Key != "" {
			path = fmt.Sprintf("%s/%s", s.Bucket, s.Key)
		} else {
			path = s.Bucket
		}
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
		Path:   "/" + path,
	}

	if s.AccessKey != "" {
		u.User = url.User(s.AccessKey)
	}

	return u.String()
}
