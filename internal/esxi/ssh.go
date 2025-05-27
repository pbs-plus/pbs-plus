//go:build linux

package esxi

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
)

// connectSSH establishes SSH connection for remote execution
func (g *GhettoVCB) connectSSH() error {
	var auth []ssh.AuthMethod

	if g.sshConfig.Password != "" {
		auth = append(auth, ssh.Password(g.sshConfig.Password))
	}

	if g.sshConfig.KeyFile != "" {
		key, err := os.ReadFile(g.sshConfig.KeyFile)
		if err != nil {
			return fmt.Errorf("unable to read private key: %w", err)
		}

		signer, err := ssh.ParsePrivateKey(key)
		if err != nil {
			return fmt.Errorf("unable to parse private key: %w", err)
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	config := &ssh.ClientConfig{
		User:            g.sshConfig.Username,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         g.sshConfig.Timeout,
	}

	if g.sshConfig.Timeout == 0 {
		config.Timeout = 30 * time.Second
	}

	addr := fmt.Sprintf("%s:%d", g.sshConfig.Host, g.sshConfig.Port)
	if g.sshConfig.Port == 0 {
		addr = fmt.Sprintf("%s:22", g.sshConfig.Host)
	}

	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	g.sshClient = client
	return nil
}
