package jobs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/log"
)

func RunShellScript(
	ctx context.Context,
	scriptFilePath string,
	envVars []string,
) (string, map[string]string, error) {
	return runShellScript(ctx, scriptFilePath, envVars, nil)
}

func RunShellScriptWithOutput(
	ctx context.Context,
	scriptFilePath string,
	envVars []string,
	onLine func(string),
) (string, map[string]string, error) {
	return runShellScript(ctx, scriptFilePath, envVars, onLine)
}

func runShellScript(
	ctx context.Context,
	scriptFilePath string,
	envVars []string,
	onLine func(string),
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
		log.Error(err, "")
	}
	defer func() {
		if err := os.Remove(envFilePath); err != nil && !os.IsNotExist(err) {
			log.Error(err, "")
		}
	}()

	cmd := exec.CommandContext(ctx, interpreter, scriptFilePath, envFilePath)
	cmd.Env = append(os.Environ(), envVars...)

	output := &scriptOutput{onLine: onLine}
	cmd.Stdout = output
	cmd.Stderr = output

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
				log.Error(err, "")
			}
		}
		<-done
		runErr = ctx.Err()
	case runErr = <-done:
	}
	output.Flush()

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
			return output.String(), resultEnvs, runErr
		}
		return output.String(), resultEnvs, fmt.Errorf(
			"script failed: %w; output=\n%s",
			runErr, output.String(),
		)
	}
	return output.String(), resultEnvs, nil
}

type scriptOutput struct {
	mu      sync.Mutex
	output  bytes.Buffer
	pending []byte
	onLine  func(string)
}

func (w *scriptOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, _ = w.output.Write(p)
	w.pending = append(w.pending, p...)
	for {
		end := bytes.IndexByte(w.pending, '\n')
		if end < 0 {
			break
		}
		w.logLine(w.pending[:end])
		w.pending = w.pending[end+1:]
	}
	return len(p), nil
}

func (w *scriptOutput) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) > 0 {
		w.logLine(w.pending)
		w.pending = nil
	}
}

func (w *scriptOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.output.String()
}

func (w *scriptOutput) logLine(line []byte) {
	if w.onLine != nil {
		w.onLine(string(bytes.TrimSuffix(line, []byte{'\r'})))
	}
}

func getInterpreterFromShebang(scriptFilePath string) (string, error) {
	file, err := os.Open(scriptFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open script file: %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Error(err, "")
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
