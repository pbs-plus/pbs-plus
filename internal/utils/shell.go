package utils

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
)

func RunShellScript(scriptFilePath string, envVars []string) (string, map[string]string, error) {
	// Make the file executable
	if err := os.Chmod(scriptFilePath, 0755); err != nil {
		return "", nil, fmt.Errorf("failed to make script file executable: %w", err)
	}

	// Get the shebang line to determine the interpreter (more robust handling)
	interpreter, err := getInterpreterFromShebang(scriptFilePath)
	if err != nil {
		// If shebang is missing or invalid, you might default or return an error
		fmt.Fprintf(os.Stderr, "Warning: Could not determine interpreter from shebang for %s, defaulting to sh: %v\n", scriptFilePath, err)
		interpreter = "sh" // Default to sh if shebang is problematic
	}

	// Create a temporary file to capture environment variables
	envFile, err := os.CreateTemp("", "script_env_*.txt")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temporary env file: %w", err)
	}
	envFilePath := envFile.Name()
	envFile.Close() // Close the file handle, the script will open and write to it

	// Ensure the temporary file is removed afterwards
	defer os.Remove(envFilePath)

	// Execute the script, passing the temporary file path as the first argument
	cmd := exec.Command(interpreter, scriptFilePath, envFilePath)

	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, envVars...)

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out // Capture both stdout and stderr

	err = cmd.Run()

	// Read the environment variables from the temporary file
	envContent, readErr := os.ReadFile(envFilePath)
	if readErr != nil {
		// Log the error but proceed with potential script execution error
		fmt.Fprintf(os.Stderr, "Warning: Failed to read environment file %s: %v\n", envFilePath, readErr)
	}

	// Parse the environment variables from the file content
	envVarsMap := make(map[string]string)
	if len(envContent) > 0 {
		envLines := strings.Split(string(envContent), "\n")
		for _, line := range envLines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") { // Ignore empty lines and comments
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				envVarsMap[parts[0]] = parts[1]
			}
		}
	}

	if err != nil {
		// Return the captured output, the potentially incomplete environment, and the error
		return out.String(), envVarsMap, fmt.Errorf("failed to run script %s: %w, output: %s", scriptFilePath, err, out.String())
	}

	return out.String(), envVarsMap, nil
}

// Helper function to extract interpreter from shebang
func getInterpreterFromShebang(scriptFilePath string) (string, error) {
	file, err := os.Open(scriptFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to open script file: %w", err)
	}
	defer file.Close()

	header := make([]byte, 100) // Read a small header
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
			return parts[0], nil // Return the first part as the interpreter command
		}
	}

	return "", fmt.Errorf("no valid shebang found in %s", scriptFilePath)
}

func SaveScriptToFile(scriptContent string) (string, error) {
	// Ensure the directory exists, create it if necessary
	if err := os.MkdirAll(constants.ScriptsBasePath, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", constants.ScriptsBasePath, err)
	}

	// Create a temporary file *within* the specified directory.
	// os.CreateTemp handles generating a unique name.
	tmpfile, err := os.CreateTemp(constants.ScriptsBasePath, "script-*.sh")
	if err != nil {
		return "", fmt.Errorf("failed to create file in directory %s: %w", constants.ScriptsBasePath, err)
	}
	defer tmpfile.Close() // Close the file when done

	// Write the script content to the file
	if _, err := tmpfile.WriteString(scriptContent); err != nil {
		os.Remove(tmpfile.Name()) // Clean up the partial file on error
		return "", fmt.Errorf("failed to write script to file %s: %w", tmpfile.Name(), err)
	}

	return tmpfile.Name(), nil
}

func UpdateScriptContentToFile(filePath string, newScriptContent string) error {
	// Resolve the absolute path of the file
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path for %s: %w", filePath, err)
	}

	// Ensure the file is within the safe directory
	if !strings.HasPrefix(absPath, constants.ScriptsBasePath) {
		return fmt.Errorf("invalid file path: %s is outside the allowed directory", absPath)
	}

	// Write the new script content to the file
	err = os.WriteFile(absPath, []byte(newScriptContent), 0644)
	if err != nil {
		return fmt.Errorf("failed to update file %s: %w", absPath, err)
	}
	return nil
}

func ReadScriptContentFromFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	return string(content), nil
}

func IsValidShellScriptWithShebang(scriptContent string) bool {
	if scriptContent == "" {
		return false
	}
	lines := strings.Split(scriptContent, "\n")
	if len(lines) == 0 {
		return false
	}
	firstLine := lines[0]
	return strings.HasPrefix(firstLine, "#!") && len(strings.TrimSpace(firstLine)) > 2
}
