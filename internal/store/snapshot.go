//go:build linux

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
	"golang.org/x/crypto/ssh"
)

func (storeInstance *Store) AgentSnapshot(agentTarget *Target) (bool, error) {
	privKeyDir := filepath.Join(DbBasePath, "agent_keys")
	privKeyFile := filepath.Join(privKeyDir, strings.ReplaceAll(fmt.Sprintf("%s.key", agentTarget.Name), " ", "-"))

	pemBytes, err := os.ReadFile(privKeyFile)
	if err != nil {
		return false, fmt.Errorf("AgentSnapshot: error reading private key file -> %w", err)
	}

	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return false, fmt.Errorf("AgentSnapshot: error parsing private key -> %w", err)
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
		Timeout:         time.Second * 2,
	}

	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:%s", agentHost, agentPort), sshConfig)
	if err != nil {
		return false, fmt.Errorf("AgentSnapshot: error dialing ssh client -> %w", err)
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return false, fmt.Errorf("AgentSnapshot: error creating new ssh session -> %w", err)
	}
	defer session.Close()

	resp, err := session.SendRequest("snapshot", true, []byte(agentDrive))
	if err != nil {
		return false, fmt.Errorf("AgentSnapshot: error sending ping request -> %w", err)
	}

	return resp, nil
}
