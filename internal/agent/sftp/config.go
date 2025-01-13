//go:build windows
// +build windows

package sftp

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	ServerKey    []byte `json:"server_key"`
	Server       string `json:"server"`
	BasePath     string `json:"base_path"`
}

var (
	InitializedConfigs sync.Map
	httpClient         = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: true},
			DisableCompression:  true,
			MaxIdleConnsPerHost: 1,
		},
	}
)

func (s *SFTPConfig) registryKey() string {
	return fmt.Sprintf("Software\\PBSPlus\\Config\\SFTP-%s", s.BasePath)
}

func InitializeSFTPConfig(driveLetter string) error {
	baseKey, err := registry.OpenKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		return fmt.Errorf("init SFTP config: %w", err)
	}
	defer baseKey.Close()

	server, _, err := baseKey.GetStringValue("ServerURL")
	if err != nil {
		return fmt.Errorf("get server URL: %w", err)
	}

	config := &SFTPConfig{
		BasePath: driveLetter,
		Server:   server,
	}

	key, err := registry.OpenKey(registry.LOCAL_MACHINE, config.registryKey(), registry.QUERY_VALUE)
	if err == nil {
		if basePath, _, err := key.GetStringValue("BasePath"); err == nil {
			config.BasePath = basePath
		}
		key.Close()
	}

	InitializedConfigs.Store(driveLetter, config)
	return config.populateKeys()
}

func (config *SFTPConfig) populateKeys() error {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, config.registryKey(), registry.QUERY_VALUE)
	if err == nil {
		if err := config.loadExistingKeys(key); err == nil {
			key.Close()
			return config.setupServerConfig()
		}
		key.Close()
	}

	if err := config.generateNewKeys(); err != nil {
		return fmt.Errorf("generate new keys: %w", err)
	}

	return config.setupServerConfig()
}

func (config *SFTPConfig) loadExistingKeys(key registry.Key) error {
	keys := map[string]*[]byte{
		"PrivateKey": &config.PrivateKey,
		"PublicKey":  &config.PublicKey,
		"ServerKey":  &config.ServerKey,
	}

	for name, dest := range keys {
		encrypted, _, err := key.GetStringValue(name)
		if err != nil {
			return err
		}

		decrypted, err := dpapi.Decrypt(encrypted)
		if err != nil {
			return err
		}

		*dest, err = base64.StdEncoding.DecodeString(decrypted)
		if err != nil {
			return err
		}
	}

	return nil
}

func (config *SFTPConfig) generateNewKeys() error {
	var err error
	config.PrivateKey, config.PublicKey, err = utils.GenerateKeyPair(4096)
	if err != nil {
		return fmt.Errorf("generate key pair: %w", err)
	}

	serverKey, err := config.getServerPublicKey()
	if err != nil {
		return fmt.Errorf("get server public key: %w", err)
	}
	config.ServerKey = []byte(*serverKey)

	return config.saveKeys()
}

func (config *SFTPConfig) saveKeys() error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, config.registryKey(), registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("create registry key: %w", err)
	}
	defer key.Close()

	keys := map[string][]byte{
		"PrivateKey": config.PrivateKey,
		"PublicKey":  config.PublicKey,
		"ServerKey":  config.ServerKey,
	}

	for name, data := range keys {
		encrypted, err := dpapi.Encrypt(base64.StdEncoding.EncodeToString(data))
		if err != nil {
			return fmt.Errorf("encrypt %s: %w", name, err)
		}

		if err := key.SetStringValue(name, encrypted); err != nil {
			return fmt.Errorf("save %s: %w", name, err)
		}
	}

	return nil
}

func (config *SFTPConfig) setupServerConfig() error {
	signer, err := ssh.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("parse private key: %w", err)
	}

	serverKey, _, _, _, err := ssh.ParseAuthorizedKey(config.ServerKey)
	if err != nil {
		return fmt.Errorf("parse server key: %w", err)
	}

	config.ServerConfig = &ssh.ServerConfig{
		NoClientAuth:  false,
		ServerVersion: "SSH-2.0-PBS-D2D-Agent",
		AuthLogCallback: func(conn ssh.ConnMetadata, method string, err error) {
			if err != nil {
				log.Printf("Auth attempt from %s@%s", conn.User(), conn.RemoteAddr())
			} else {
				log.Printf("Auth accepted from %s@%s", conn.User(), conn.RemoteAddr())
			}
		},
		PublicKeyCallback: func(conn ssh.ConnMetadata, auth ssh.PublicKey) (*ssh.Permissions, error) {
			if string(serverKey.Marshal()) == string(auth.Marshal()) {
				return &ssh.Permissions{
					Extensions: map[string]string{
						"pubkey-fp": ssh.FingerprintSHA256(auth),
					},
				}, nil
			}
			return nil, fmt.Errorf("unknown public key for %s", conn.RemoteAddr())
		},
	}
	config.ServerConfig.AddHostKey(signer)
	return nil
}

func (config *SFTPConfig) getServerPublicKey() (*string, error) {
	hostname, _ := os.Hostname()
	reqBody, err := json.Marshal(map[string]string{
		"public_key": string(config.PublicKey),
		"base_path":  config.BasePath,
		"hostname":   hostname,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	resp, err := httpClient.Post(
		fmt.Sprintf("%s/api2/json/d2d/target", strings.TrimSuffix(config.Server, "/")),
		"application/json",
		strings.NewReader(string(reqBody)),
	)
	if err != nil {
		return nil, fmt.Errorf("server request: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		PublicKey string `json:"public_key"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &result.PublicKey, nil
}
