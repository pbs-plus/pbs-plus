//go:build windows
// +build windows

package sftp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/billgraziano/dpapi"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/crypto/ssh"
	"golang.org/x/sys/windows/registry"
)

type SFTPConfig struct {
	ServerConfig *ssh.ServerConfig
	PrivateKey   []byte `json:"private_key"`
	PublicKey    []byte `json:"public_key"`
	Server       string `json:"server"`
	BasePath     string `json:"base_path"`
}

var (
	InitializedConfigs sync.Map
	httpClient         = &http.Client{
		Timeout: 5 * time.Second, // Reduced timeout since we'll call this more frequently
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			DisableCompression:  true,
			DisableKeepAlives:   false,
			MaxIdleConnsPerHost: 20, // Increased for more concurrent connections
			IdleConnTimeout:     30 * time.Second,
		},
	}
)

func (s *SFTPConfig) GetRegistryKey() string {
	return fmt.Sprintf("Software\\PBSPlus\\Config\\SFTP-%s", s.BasePath)
}

func InitializeSFTPConfig(ctx context.Context, driveLetter string) error {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		return fmt.Errorf("initialize SFTP config: %w", err)
	}
	defer baseKey.Close()

	server, _, err := baseKey.GetStringValue("ServerURL")
	if err != nil {
		return fmt.Errorf("get server URL: %w", err)
	}

	newSftpConfig := &SFTPConfig{
		BasePath: driveLetter,
		Server:   server,
	}

	key, err := registry.OpenKey(registry.LOCAL_MACHINE, newSftpConfig.GetRegistryKey(), registry.QUERY_VALUE)
	if err == nil {
		if basePath, _, err := key.GetStringValue("BasePath"); err == nil {
			newSftpConfig.BasePath = basePath
		}
		key.Close()
	}

	InitializedConfigs.Store(driveLetter, newSftpConfig)
	return newSftpConfig.PopulateKeys(ctx)
}

func (config *SFTPConfig) PopulateKeys(ctx context.Context) error {
	configSSH := &ssh.ServerConfig{
		NoClientAuth:  false,
		ServerVersion: "SSH-2.0-PBS-D2D-Agent",
		AuthLogCallback: func(conn ssh.ConnMetadata, method string, err error) {
			if err != nil {
				log.Printf("Agent: %s Authentication Attempt from %s", conn.User(), conn.RemoteAddr())
			} else {
				log.Printf("Agent: %s Authentication Accepted from %s", conn.User(), conn.RemoteAddr())
			}
		},
	}

	if err := config.loadOrGenerateKeys(); err != nil {
		return fmt.Errorf("load or generate keys: %w", err)
	}

	return config.finalizeConfig(ctx, configSSH)
}

func (config *SFTPConfig) loadOrGenerateKeys() error {
	if err := config.loadExistingKeys(); err == nil {
		return nil
	}

	// Generate new keys if loading fails
	privateKey, pubKey, err := utils.GenerateKeyPair(4096)
	if err != nil {
		return fmt.Errorf("generate SSH key pair: %w", err)
	}

	config.PrivateKey = privateKey
	config.PublicKey = pubKey

	return config.storeKeys()
}

func (config *SFTPConfig) loadExistingKeys() error {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, config.GetRegistryKey(), registry.QUERY_VALUE)
	if err != nil {
		return err
	}
	defer key.Close()

	keys := []struct {
		name   string
		target *[]byte
	}{
		{"PrivateKey", &config.PrivateKey},
		{"PublicKey", &config.PublicKey},
	}

	for _, k := range keys {
		if err := loadAndDecryptKey(key, k.name, k.target); err != nil {
			return err
		}
	}

	return nil
}

func loadAndDecryptKey(key registry.Key, valueName string, target *[]byte) error {
	encrypted, _, err := key.GetStringValue(valueName)
	if err != nil {
		return err
	}

	decrypted, err := dpapi.Decrypt(encrypted)
	if err != nil {
		return fmt.Errorf("decrypt %s: %w", valueName, err)
	}

	*target, err = base64.StdEncoding.DecodeString(decrypted)
	if err != nil {
		return fmt.Errorf("decode %s: %w", valueName, err)
	}

	return nil
}

func (config *SFTPConfig) storeKeys() error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, config.GetRegistryKey(), registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("create registry key: %w", err)
	}
	defer key.Close()

	keys := []struct {
		name  string
		value []byte
	}{
		{"PrivateKey", config.PrivateKey},
		{"PublicKey", config.PublicKey},
	}

	for _, k := range keys {
		if err := storeEncryptedKey(key, k.name, k.value); err != nil {
			return err
		}
	}

	return nil
}

func storeEncryptedKey(key registry.Key, valueName string, data []byte) error {
	encrypted, err := dpapi.Encrypt(base64.StdEncoding.EncodeToString(data))
	if err != nil {
		return fmt.Errorf("encrypt %s: %w", valueName, err)
	}

	if err := key.SetStringValue(valueName, encrypted); err != nil {
		return fmt.Errorf("save %s: %w", valueName, err)
	}

	return nil
}

func (config *SFTPConfig) finalizeConfig(ctx context.Context, configSSH *ssh.ServerConfig) error {
	parsedSigner, err := ssh.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("parse private key: %w", err)
	}
	configSSH.AddHostKey(parsedSigner)

	// Get server key and set up callback
	configSSH.PublicKeyCallback = func(conn ssh.ConnMetadata, auth ssh.PublicKey) (*ssh.Permissions, error) {
		serverKey, err := config.getServerPublicKey(ctx, string(config.PublicKey))
		if err != nil {
			return nil, fmt.Errorf("fetch server key: %w", err)
		}

		parsedServerKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(*serverKey))
		if err != nil {
			return nil, fmt.Errorf("parse server key: %w", err)
		}

		if bytes.Equal(parsedServerKey.Marshal(), auth.Marshal()) {
			return &ssh.Permissions{
				Extensions: map[string]string{
					"pubkey-fp": ssh.FingerprintSHA256(auth),
				},
			}, nil
		}
		return nil, fmt.Errorf("invalid public key for %s", conn.RemoteAddr())
	}

	config.ServerConfig = configSSH
	return nil
}

type NewAgentResp struct {
	PublicKey string `json:"public_key"`
}

func (config *SFTPConfig) getServerPublicKey(ctx context.Context, publicKey string) (*string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("get hostname: %w", err)
	}

	reqBody, err := json.Marshal(map[string]string{
		"public_key": publicKey,
		"base_path":  config.BasePath,
		"hostname":   hostname,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/api2/json/d2d/target", strings.TrimSuffix(config.Server, "/")),
		bytes.NewReader(reqBody),
	)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var newAgentStruct NewAgentResp
	if err := json.Unmarshal(body, &newAgentStruct); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	return &newAgentStruct.PublicKey, nil
}
