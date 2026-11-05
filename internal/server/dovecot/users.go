//go:build linux

package dovecot

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/coredb"
)

const (
	apiUser          = "doveadm"
	apiTag           = "pbs-plus"
	apiResponseLimit = 64 << 20
)

// ListUsers enumerates the target's mailbox users over the doveadm HTTP API.
// dsync has no userdb iteration of its own, so server-scope backups need the
// API listener in addition to the dsync port.
func ListUsers(ctx context.Context, target coredb.Target, password string) ([]string, error) {
	if !target.IsDovecot() {
		return nil, errors.New("target is not a Dovecot target")
	}
	if target.DovecotAPIPort < 1 || target.DovecotAPIPort > 65535 {
		return nil, errors.New("Dovecot target has no doveadm HTTP API port; server-scope backups need one")
	}
	if _, err := passwordSetting(password); err != nil {
		return nil, err
	}
	tlsConfig, err := apiTLSConfig(target)
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal([]any{[]any{"user", map[string]any{"userMask": []string{"*"}}, apiTag}})
	if err != nil {
		return nil, fmt.Errorf("encode Dovecot user query: %w", err)
	}
	endpoint := "https://" + net.JoinHostPort(target.DatabaseHost, strconv.Itoa(target.DovecotAPIPort)) + "/doveadm/v1"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build Dovecot user query: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth(apiUser, password)

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("query Dovecot users: %w", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			log.Error(err, "")
		}
	}()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("query Dovecot users: doveadm API returned %s", response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, apiResponseLimit))
	if err != nil {
		return nil, fmt.Errorf("read Dovecot user list: %w", err)
	}
	return parseUserList(data)
}

func apiTLSConfig(target coredb.Target) (*tls.Config, error) {
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: target.DatabaseHost}
	if target.DatabaseCACertificate == "" {
		return config, nil
	}
	pem, err := readCACertificate(target.DatabaseCACertificate)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("Dovecot target CA certificate contains no usable certificate")
	}
	config.RootCAs = pool
	return config, nil
}

func parseUserList(data []byte) ([]string, error) {
	var envelopes [][]json.RawMessage
	if err := json.Unmarshal(data, &envelopes); err != nil {
		return nil, fmt.Errorf("decode Dovecot user list: %w", err)
	}
	usernames := make([]string, 0, len(envelopes))
	for _, envelope := range envelopes {
		if len(envelope) < 2 {
			return nil, errors.New("decode Dovecot user list: malformed doveadm response")
		}
		var kind string
		if err := json.Unmarshal(envelope[0], &kind); err != nil {
			return nil, fmt.Errorf("decode Dovecot user list: %w", err)
		}
		if kind != "doveadmResponse" {
			return nil, fmt.Errorf("query Dovecot users: doveadm returned %s %s", kind, envelope[1])
		}
		entries, err := parseUserEntries(envelope[1])
		if err != nil {
			return nil, err
		}
		usernames = append(usernames, entries...)
	}
	slices.Sort(usernames)
	return slices.Compact(usernames), nil
}

// parseUserEntries reads every doveadm payload shape: a wildcard lookup answers
// with a userList array, a single lookup with a map keyed by username, and some
// versions with rows carrying a username field.
func parseUserEntries(payload json.RawMessage) ([]string, error) {
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(payload, &rows); err == nil {
		usernames := make([]string, 0, len(rows))
		for _, row := range rows {
			for _, field := range []string{"username", "user"} {
				raw, ok := row[field]
				if !ok {
					continue
				}
				var username string
				if err := json.Unmarshal(raw, &username); err != nil {
					return nil, fmt.Errorf("decode Dovecot user list: %w", err)
				}
				if username != "" {
					usernames = append(usernames, username)
				}
				break
			}
		}
		return usernames, nil
	}

	var keyed map[string]json.RawMessage
	if err := json.Unmarshal(payload, &keyed); err != nil {
		return nil, fmt.Errorf("decode Dovecot user list: %w", err)
	}
	usernames := make([]string, 0, len(keyed))
	for key, raw := range keyed {
		var list []string
		if err := json.Unmarshal(raw, &list); err == nil {
			for _, username := range list {
				if username != "" {
					usernames = append(usernames, username)
				}
			}
			continue
		}
		if key != "" {
			usernames = append(usernames, key)
		}
	}
	return usernames, nil
}
