package esxi

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// connectSSH establishes SSH connection for remote execution
func (g *GhettoVCB) connectSSH() error {
	var auth []ssh.AuthMethod

	// ESXi uses keyboard-interactive for password auth, not regular password
	if g.sshConfig.Password != "" {
		auth = append(auth, ssh.KeyboardInteractive(func(user, instruction string, questions []string, echos []bool) ([]string, error) {
			answers := make([]string, len(questions))
			for i := range questions {
				answers[i] = g.sshConfig.Password
			}
			return answers, nil
		}))
	}

	if g.sshConfig.KeyFile != "" {
		key, err := os.ReadFile(g.sshConfig.KeyFile)
		if err != nil {
			return fmt.Errorf("unable to read private key: %w", err)
		}

		var signer ssh.Signer
		if g.sshConfig.KeyPassphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(g.sshConfig.KeyPassphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(key)
		}

		if err != nil {
			return fmt.Errorf("unable to parse private key: %w", err)
		}

		auth = append(auth, ssh.PublicKeys(signer))
	}

	if len(auth) == 0 {
		return fmt.Errorf("no authentication methods configured")
	}

	config := &ssh.ClientConfig{
		User:            g.sshConfig.Username,
		Auth:            auth,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         g.sshConfig.Timeout,
		// Match the algorithms that worked in your SSH session
		Config: ssh.Config{
			KeyExchanges: []string{
				"ecdh-sha2-nistp256",
				"ecdh-sha2-nistp384",
				"ecdh-sha2-nistp521",
				"diffie-hellman-group-exchange-sha256",
				"diffie-hellman-group16-sha512",
				"diffie-hellman-group18-sha512",
				"diffie-hellman-group14-sha256",
			},
			Ciphers: []string{
				"aes256-gcm@openssh.com",
				"aes128-gcm@openssh.com",
				"aes256-ctr",
				"aes192-ctr",
				"aes128-ctr",
			},
			MACs: []string{
				"hmac-sha2-256",
				"hmac-sha2-512",
			},
		},
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

// executeCommand executes a command via SSH
func (g *GhettoVCB) executeCommand(cmd string) (string, error) {
	if g.sshClient == nil {
		return "", fmt.Errorf("SSH client not connected")
	}

	session, err := g.sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()

	output, err := session.CombinedOutput(cmd)
	return string(output), err
}

func (g *GhettoVCB) executeCommandStream(cmd string) (<-chan string, func() error, error) {
	if g.sshClient == nil {
		return nil, nil, fmt.Errorf("SSH client not connected")
	}

	session, err := g.sshClient.NewSession()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create session: %w", err)
	}
	// Note: session.Close() is handled carefully in the goroutine below

	stdoutPipe, err := session.StdoutPipe()
	if err != nil {
		session.Close() // Close session if pipe creation fails
		return nil, nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	stderrPipe, err := session.StderrPipe()
	if err != nil {
		session.Close() // Close session if pipe creation fails
		return nil, nil, fmt.Errorf("failed to get stderr pipe: %w", err)
	}

	outputChan := make(chan string, 100) // Buffered channel
	var cmdErr error
	var wg sync.WaitGroup

	// Start the command
	if err := session.Start(cmd); err != nil {
		session.Close() // Close session if start fails
		return nil, nil, fmt.Errorf("failed to start command: %w", err)
	}

	// Goroutine to read from stdout
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			outputChan <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error scanning stdout: %v", err)
		}
	}()

	// Goroutine to read from stderr
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			outputChan <- "[STDERR] " + scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Error scanning stderr: %v", err)
		}
	}()

	var finalErrorMu sync.Mutex
	var finalError error

	go func() {
		cmdErr = session.Wait()
		finalErrorMu.Lock()
		finalError = cmdErr
		finalErrorMu.Unlock()

		wg.Wait()

		session.Close()

		close(outputChan)
	}()

	getFinalError := func() error {
		finalErrorMu.Lock()
		defer finalErrorMu.Unlock()
		return finalError
	}

	return outputChan, getFinalError, nil
}
