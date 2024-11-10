//go:build windows
// +build windows

package sftp

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
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

func (s *SFTPConfig) GetRegistryKey() string {
	return fmt.Sprintf("Software\\PBSPlus\\Config\\SFTP-%s", s.BasePath)
}

var logger *syslog.Logger

func InitializeSFTPConfig(svc service.Service, driveLetter string) (*SFTPConfig, error) {
	var err error
	if logger == nil {
		logger, err = syslog.InitializeLogger(svc)
		if err != nil {
			return nil, fmt.Errorf("InitializeLogger: failed to initialize logger -> %w", err)
		}
	}

	baseKey, _, err := registry.CreateKey(registry.LOCAL_MACHINE, "Software\\PBSPlus\\Config", registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("InitializeSFTPConfig: unable to create registry key -> %w", err)
	}

	defer baseKey.Close()

	var server string
	if server, _, err = baseKey.GetStringValue("ServerURL"); err != nil {
		return nil, fmt.Errorf("InitializeSFTPConfig: unable to get server url -> %w", err)
	}

	newSftpConfig := &SFTPConfig{
		BasePath: driveLetter,
		Server:   server,
	}

	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, newSftpConfig.GetRegistryKey(), registry.QUERY_VALUE)
	if err != nil {
		return nil, fmt.Errorf("InitializeSFTPConfig: unable to create registry key -> %w", err)
	}

	defer key.Close()

	if basePath, _, err := key.GetStringValue("BasePath"); err == nil {
		newSftpConfig.BasePath = basePath
	}

	return newSftpConfig, nil
}

func (config *SFTPConfig) PopulateKeys() error {
	configSSH := &ssh.ServerConfig{
		NoClientAuth:  false,
		ServerVersion: "SSH-2.0-PBS-D2D-Agent",
		AuthLogCallback: func(conn ssh.ConnMetadata, method string, err error) {
			logAuthAttempt(conn, method, err)
		},
	}

	key, err := registry.OpenKey(registry.LOCAL_MACHINE, config.GetRegistryKey(), registry.QUERY_VALUE)
	if err == nil {
		defer key.Close()

		if privateKey, _, err := key.GetStringValue("PrivateKey"); err == nil {
			config.PrivateKey, err = base64.StdEncoding.DecodeString(privateKey)
			if err != nil {
				return fmt.Errorf("PopulateKeys: failed to decode private key -> %w", err)
			}
		}

		if publicKey, _, err := key.GetStringValue("PublicKey"); err == nil {
			config.PublicKey, err = base64.StdEncoding.DecodeString(publicKey)
			if err != nil {
				return fmt.Errorf("PopulateKeys: failed to decode public key -> %w", err)
			}
		}

		if serverKey, _, err := key.GetStringValue("ServerKey"); err == nil {
			config.ServerKey, err = base64.StdEncoding.DecodeString(serverKey)
			if err != nil {
				return fmt.Errorf("PopulateKeys: failed to decode server key -> %w", err)
			}
		}
	}

	if len(config.PrivateKey) == 0 || len(config.PublicKey) == 0 || len(config.ServerKey) == 0 {
		privateKey, pubKey, err := utils.GenerateKeyPair(4096)
		if err != nil {
			return fmt.Errorf("PopulateKeys: failed to generate SSH key pair -> %w", err)
		}

		config.PrivateKey = privateKey
		config.PublicKey = pubKey

		serverKey, err := getServerPublicKey(config.Server, string(pubKey), config.BasePath)
		if err != nil {
			return fmt.Errorf("PopulateKeys: failed to get server public ssh key -> %w", err)
		}

		config.ServerKey = []byte(*serverKey)

		key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, config.GetRegistryKey(), registry.ALL_ACCESS)
		if err != nil {
			return fmt.Errorf("PopulateKeys: failed to create registry key -> %w", err)
		}
		defer key.Close()

		if err := key.SetStringValue("PrivateKey", base64.StdEncoding.EncodeToString(config.PrivateKey)); err != nil {
			return fmt.Errorf("PopulateKeys: failed to save private key -> %w", err)
		}
		if err := key.SetStringValue("PublicKey", base64.StdEncoding.EncodeToString(config.PublicKey)); err != nil {
			return fmt.Errorf("PopulateKeys: failed to save public key -> %w", err)
		}
		if err := key.SetStringValue("ServerKey", base64.StdEncoding.EncodeToString(config.ServerKey)); err != nil {
			return fmt.Errorf("PopulateKeys: failed to save server key -> %w", err)
		}
	}

	parsedSigner, err := ssh.ParsePrivateKey(config.PrivateKey)
	if err != nil {
		return fmt.Errorf("PopulateKeys: failed to parse private key to signer -> %w", err)
	}
	configSSH.AddHostKey(parsedSigner)

	parsedServerKey, _, _, _, err := ssh.ParseAuthorizedKey(config.ServerKey)
	if err != nil {
		return fmt.Errorf("PopulateKeys: failed to parse server key -> %w", err)
	}

	configSSH.PublicKeyCallback = func(conn ssh.ConnMetadata, auth ssh.PublicKey) (*ssh.Permissions, error) {
		if comparePublicKeys(parsedServerKey, auth) {
			return &ssh.Permissions{
				Extensions: map[string]string{
					"pubkey-fp": ssh.FingerprintSHA256(auth),
				},
			}, nil
		}
		return nil, fmt.Errorf("PopulateKeys: unknown public key for %s -> %w", conn.RemoteAddr().String(), err)
	}

	config.ServerConfig = configSSH
	return nil
}

func logAuthAttempt(conn ssh.ConnMetadata, _ string, err error) {
	if err != nil {
		log.Printf("Agent: %s Authentication Attempt from %s", conn.User(), conn.RemoteAddr())
	} else {
		log.Printf("Agent: %s Authentication Accepted from %s", conn.User(), conn.RemoteAddr())
	}
}

type NewAgentResp struct {
	PublicKey string `json:"public_key"`
}

func getServerPublicKey(server string, publicKey string, basePath string) (*string, error) {
	hostname, _ := os.Hostname()
	reqBody, err := json.Marshal(map[string]string{
		"public_key": publicKey,
		"base_path":  basePath,
		"hostname":   hostname,
	})
	if err != nil {
		return nil, fmt.Errorf("getServerPublicKey: error json marshal for body request -> %w", err)
	}

	newAgentReq, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf(
			"%s/api2/json/d2d/target",
			strings.TrimSuffix(server, "/"),
		),
		bytes.NewBuffer(reqBody),
	)

	client := http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	newAgentResp, err := client.Do(newAgentReq)
	if err != nil {
		return nil, fmt.Errorf("getServerPublicKey: error executing http request -> %w", err)
	}

	defer func() {
		_, _ = io.Copy(io.Discard, newAgentResp.Body)
		newAgentResp.Body.Close()
	}()

	newAgentBody, err := io.ReadAll(newAgentResp.Body)
	if err != nil {
		return nil, fmt.Errorf("getServerPublicKey: error getting body from response -> %w", err)
	}

	var newAgentStruct NewAgentResp
	err = json.Unmarshal(newAgentBody, &newAgentStruct)
	if err != nil {
		return nil, fmt.Errorf("getServerPublicKey: error unmarshal json from body -> %w", err)
	}

	return &newAgentStruct.PublicKey, nil
}

func comparePublicKeys(key1, key2 ssh.PublicKey) bool {
	return string(key1.Marshal()) == string(key2.Marshal())
}
