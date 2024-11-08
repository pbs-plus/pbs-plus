package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
	"golang.org/x/crypto/ssh"
)

func (storeInstance *Store) AgentPing(agentTarget *Target) (bool, error) {
	privKeyDir := filepath.Join(DbBasePath, "agent_keys")
	privKeyFile := filepath.Join(privKeyDir, strings.ReplaceAll(fmt.Sprintf("%s.key", agentTarget.Name), " ", "-"))

	pemBytes, err := os.ReadFile(privKeyFile)
	if err != nil {
		return false, fmt.Errorf("AgentPing: error reading private key file -> %w", err)
	}

	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return false, fmt.Errorf("AgentPing: error parsing private key -> %w", err)
	}

	agentPath := strings.TrimPrefix(agentTarget.Path, "agent://")
	agentPathParts := strings.Split(agentPath, "/")
	agentHost := agentPathParts[0]
	agentDrive := agentPathParts[1]
	agentDriveRune := []rune(agentDrive)[0]
	agentPort, err := utils.DriveLetterPort(agentDriveRune)

	sshConfig := &ssh.ClientConfig{
		User:            "proxmox",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", agentHost, agentPort), sshConfig)
	if err != nil {
		return false, fmt.Errorf("AgentPing: error dialing ssh client -> %w", err)
	}
	defer client.Close()

	_, pong, err := client.SendRequest("ping", true, []byte{})
	if err != nil {
		return false, fmt.Errorf("AgentPing: error sending ping request -> %w", err)
	}

	return bytes.Equal(pong, []byte("pong")), nil
}
