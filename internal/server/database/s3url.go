//go:build linux

package database

import (
	"fmt"
	"net/url"
	"strings"
)

type S3Url struct {
	Scheme      string
	UseSSL      bool
	Endpoint    string
	Region      string
	AccessKey   string
	Bucket      string
	Key         string
	IsPathStyle bool
}

func ParseS3Url(raw string) (*S3Url, error) {
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

	if u.User != nil {
		s3.AccessKey = u.User.Username()
	}

	pathParts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")

	isVirtualHost := false
	var bucketFromHost string

	if dotIndex := strings.Index(u.Host, "."); dotIndex > 0 {
		potentialBucket := u.Host[:dotIndex]
		remainingHost := u.Host[dotIndex+1:]

		if len(pathParts) == 0 || (len(pathParts) == 1 && pathParts[0] == "") {
			// No path, so bucket must be in hostname
			isVirtualHost = true
			bucketFromHost = potentialBucket
			s3.Endpoint = remainingHost
		} else {

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
		s3.Bucket = bucketFromHost
		if len(pathParts) > 0 && pathParts[0] != "" {
			s3.Key = strings.Join(pathParts, "/")
		}
	} else {
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
