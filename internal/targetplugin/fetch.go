package targetplugin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	DefaultFetchTimeout       = 30 * time.Second
	repositorySignatureSuffix = ".sig"
)

// ErrRepositoryUnchanged reports that a conditional refresh matched the cached index.
var ErrRepositoryUnchanged = errors.New("plugin repository index is unchanged")

// Fetcher downloads repository documents over HTTPS with bounded sizes.
type Fetcher struct {
	Client *http.Client
}

// PluginRepositoryCache carries the validators stored by the last successful refresh.
type PluginRepositoryCache struct {
	ETag         string
	LastModified string
}

// RepositoryDocument carries authenticated bytes and cache validators for one index.
type RepositoryDocument struct {
	URL          string
	Index        []byte
	Signature    []byte
	ETag         string
	LastModified string
}

// Index downloads a repository index and its detached signature, honoring cache validators.
func (fetcher Fetcher) Index(ctx context.Context, indexURL string, refresh PluginRepositoryCache) (RepositoryDocument, error) {
	if err := validateAbsoluteRepositoryURL(indexURL); err != nil {
		return RepositoryDocument{}, err
	}
	headers := map[string]string{}
	if refresh.ETag != "" {
		headers["If-None-Match"] = refresh.ETag
	}
	if refresh.LastModified != "" {
		headers["If-Modified-Since"] = refresh.LastModified
	}
	index, response, err := fetcher.get(ctx, indexURL, MaxRepositoryIndexBytes, headers)
	if err != nil {
		return RepositoryDocument{}, err
	}
	if response.StatusCode == http.StatusNotModified {
		return RepositoryDocument{}, ErrRepositoryUnchanged
	}
	signature, _, err := fetcher.get(ctx, indexURL+repositorySignatureSuffix, MaxRepositorySignatureBytes, nil)
	if err != nil {
		return RepositoryDocument{}, err
	}
	return RepositoryDocument{
		URL:          indexURL,
		Index:        index,
		Signature:    signature,
		ETag:         response.Header.Get("ETag"),
		LastModified: response.Header.Get("Last-Modified"),
	}, nil
}

// Manifest downloads one release manifest, which stays unauthenticated until its digest is verified.
func (fetcher Fetcher) Manifest(ctx context.Context, baseURL, manifestURL string) ([]byte, error) {
	resolved, err := ResolveRepositoryURL(baseURL, manifestURL)
	if err != nil {
		return nil, err
	}
	manifest, _, err := fetcher.get(ctx, resolved, MaxManifestBytes, nil)
	return manifest, err
}

// Artifact opens one artifact stream for digest and signature verification by the caller.
func (fetcher Fetcher) Artifact(ctx context.Context, baseURL, artifactURL string) (io.ReadCloser, error) {
	resolved, err := ResolveRepositoryURL(baseURL, artifactURL)
	if err != nil {
		return nil, err
	}
	response, err := fetcher.do(ctx, resolved, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		_ = response.Body.Close()
		return nil, fmt.Errorf("fetch %s: unexpected status %s", resolved, response.Status)
	}
	return response.Body, nil
}

// ResolveRepositoryURL resolves a relative release URL against its signed index location.
func ResolveRepositoryURL(baseURL, reference string) (string, error) {
	if err := validateAbsoluteRepositoryURL(baseURL); err != nil {
		return "", err
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse repository URL: %w", err)
	}
	target, err := url.Parse(reference)
	if err != nil {
		return "", fmt.Errorf("parse repository reference: %w", err)
	}
	if target.IsAbs() {
		if err := validateAbsoluteRepositoryURL(reference); err != nil {
			return "", err
		}
		return target.String(), nil
	}
	if target.Host != "" {
		return "", fmt.Errorf("repository reference %q is not HTTPS", reference)
	}
	return base.ResolveReference(target).String(), nil
}

func (fetcher Fetcher) get(ctx context.Context, target string, limit int64, headers map[string]string) ([]byte, *http.Response, error) {
	response, err := fetcher.do(ctx, target, headers)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotModified {
		return nil, response, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("fetch %s: unexpected status %s", target, response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", target, err)
	}
	if int64(len(body)) > limit {
		return nil, nil, fmt.Errorf("fetch %s: response exceeds %d bytes", target, limit)
	}
	return body, response, nil
}

func (fetcher Fetcher) do(ctx context.Context, target string, headers map[string]string) (*http.Response, error) {
	if err := validateAbsoluteRepositoryURL(target); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", target, err)
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	client := fetcher.Client
	if client == nil {
		client = &http.Client{Timeout: DefaultFetchTimeout}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", target, err)
	}
	return response, nil
}

// ValidateRepositoryURL requires an absolute HTTPS repository location.
func ValidateRepositoryURL(target string) error {
	return validateAbsoluteRepositoryURL(target)
}

func validateAbsoluteRepositoryURL(target string) error {
	parsed, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("parse repository URL: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("repository URL %q must be absolute HTTPS", target)
	}
	return nil
}
