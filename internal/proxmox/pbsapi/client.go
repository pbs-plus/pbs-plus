package pbsapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func NewLocal() *Client {
	return New(conf.ProxyTargetURL, pinnedTLSConfig(conf.CertFile))
}

func New(baseURL string, tlsConfig *tls.Config) *Client {
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: &http.Transport{TLSClientConfig: tlsConfig},
		},
	}
}

func pinnedTLSConfig(certFile string) *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("pbsapi: no peer certificate")
			}
			expected, err := loadCertificate(certFile)
			if err != nil {
				return err
			}
			if !bytes.Equal(cs.PeerCertificates[0].Raw, expected.Raw) {
				return errors.New("pbsapi: proxy certificate does not match the local PBS certificate")
			}
			return nil
		},
	}
}

func loadCertificate(path string) (*x509.Certificate, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pbsapi: read certificate: %w", err)
	}
	for {
		var block *pem.Block
		block, data = pem.Decode(data)
		if block == nil {
			return nil, errors.New("pbsapi: no certificate in " + path)
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
}

type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("pbsapi: %d %s", e.Status, e.Message)
}

func (c *Client) Get(ctx context.Context, auth *http.Request, path string, query url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path+"?"+query.Encode(), nil)
	if err != nil {
		return err
	}
	for _, header := range []string{"Cookie", "Authorization"} {
		if v := auth.Header.Get(header); v != "" {
			req.Header.Set(header, v)
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("pbsapi: %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return fmt.Errorf("pbsapi: read %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		var failure struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(body, &failure)
		if failure.Message == "" {
			failure.Message = http.StatusText(resp.StatusCode)
		}
		return &Error{Status: resp.StatusCode, Message: failure.Message}
	}
	envelope := struct {
		Data any `json:"data"`
	}{Data: out}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("pbsapi: decode %s: %w", path, err)
	}
	return nil
}
