package jobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"os"
	"os/exec"
	"strings"
)

func RunShellScript(
	ctx context.Context,
	scriptFilePath string,
	envVars []string,
) (string, map[string]string, error) {
	if err := os.Chmod(scriptFilePath, 0755); err != nil {
		return "", nil, fmt.Errorf(
			"failed to make script file executable: %w", err,
		)
	}

	interpreter, err := getInterpreterFromShebang(scriptFilePath)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"WARNING: could not read shebang, defaulting to sh: %v\n",
			err,
		)
		interpreter = "sh"
	}

	envFile, err := os.CreateTemp("", "script_env_*.txt")
	if err != nil {
		return "", nil, fmt.Errorf(
			"failed to create temporary env file: %w", err,
		)
	}
	envFilePath := envFile.Name()
	if err := envFile.Close(); err != nil {
		syslog.L.Error(err).Write()
	}
	defer func() {
		if err := os.Remove(envFilePath); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	cmd := exec.CommandContext(ctx, interpreter, scriptFilePath, envFilePath)
	cmd.Env = append(os.Environ(), envVars...)

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err := cmd.Start(); err != nil {
		return "", nil, fmt.Errorf("failed to start script: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	var runErr error
	select {
	case <-ctx.Done():
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				syslog.L.Error(err).Write()
			}
		}
		<-done
		runErr = ctx.Err()
	case runErr = <-done:
	}

	envContent, readErr := os.ReadFile(envFilePath)
	if readErr != nil {
		fmt.Fprintf(
			os.Stderr,
			"WARNING: failed to read env file %s: %v\n",
			envFilePath,
			readErr,
		)
	}
	resultEnvs := make(map[string]string)
	for line := range strings.SplitSeq(string(envContent), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			resultEnvs[parts[0]] = parts[1]
		}
	}

	if runErr != nil {
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			return outBuf.String(), resultEnvs, runErr
		}
		return outBuf.String(), resultEnvs, fmt.Errorf(
			"script failed: %w; output=\n%s",
			runErr, outBuf.String(),
		)
	}
	return outBuf.String(), resultEnvs, nil
}

func getInterpreterFromShebang(scriptFilePath string) (string, error) {
	file, err := os.Open(scriptFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open script file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			syslog.L.Error(err).Write()
		}
	}()

	header := make([]byte, 100)
	n, err := file.Read(header)
	if err != nil {
		return "", fmt.Errorf("failed to read script file header: %w", err)
	}

	headerStr := string(header[:n])
	lines := strings.Split(headerStr, "\n")
	if len(lines) > 0 && strings.HasPrefix(lines[0], "#!") {
		shebang := lines[0]
		interpreter := strings.TrimPrefix(shebang, "#!")
		interpreter = strings.TrimSpace(interpreter)
		// Consider handling arguments in the shebang like #!/usr/bin/env python
		parts := strings.Fields(interpreter)
		if len(parts) > 0 {
			return parts[0], nil
		}
	}

	return "", fmt.Errorf("no valid shebang found in %s", scriptFilePath)
}
