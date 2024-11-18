//go:build linux

package store

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

func checkAgentAuth(r *http.Request) error {
	auth := r.Header.Get("Authorization")

	privKeyDir := filepath.Join(DbBasePath, "agent_keys")

	authTok := strings.TrimPrefix(auth, "PBSPlusAPIAgent=")
	authSplit := strings.Split(authTok, ":")

	privKeyFilePath := filepath.Join(
		privKeyDir,
		fmt.Sprintf("%s.key", authSplit[0]),
	)

	privKeyFile, err := os.ReadFile(privKeyFilePath)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error opening private key file \"%s\" -> %w", privKeyFilePath, err)
	}

	pubKey, err := utils.GeneratePublicKeyFromPrivateKey(privKeyFile)
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error generating pub key \"%s\" -> %w", privKeyFilePath, err)
	}

	passedPub, err := base64.StdEncoding.DecodeString(authSplit[1])
	if err != nil {
		return fmt.Errorf("CheckAgentAuth: error pub key -> %w", err)
	}

	if !reflect.DeepEqual(pubKey, passedPub) {
		return fmt.Errorf("CheckAgentAuth: invalid auth")
	}

	return nil
}

func (storeInstance *Store) CheckProxyAuth(r *http.Request) error {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "PBSPlusAPIAgent=") {
		return checkAgentAuth(r)
	}

	checkEndpoint := "/api2/json/version"
	req, err := http.NewRequest(
		http.MethodGet,
		fmt.Sprintf(
			"%s%s",
			ProxyTargetURL,
			checkEndpoint,
		),
		nil,
	)

	if err != nil {
		return fmt.Errorf("CheckProxyAuth: error creating http request -> %w", err)
	}

	for _, cookie := range r.Cookies() {
		req.AddCookie(cookie)
	}

	if authHead := r.Header.Get("Authorization"); authHead != "" {
		req.Header.Set("Authorization", authHead)
	}

	if storeInstance.HTTPClient == nil {
		storeInstance.HTTPClient = &http.Client{
			Timeout:   time.Second * 30,
			Transport: utils.BaseTransport,
		}
	}

	resp, err := storeInstance.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("CheckProxyAuth: invalid auth -> %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		return fmt.Errorf("CheckProxyAuth: invalid auth -> %w", err)
	}

	return nil
}
