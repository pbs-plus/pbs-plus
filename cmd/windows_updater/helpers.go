//go:build windows

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"golang.org/x/sys/windows"
)

var (
	mutex  sync.Mutex
	handle windows.Handle
)

// Single-instance mutex using Windows named mutex
func createMutex() error {
	// Use executable basename as mutex name
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to get executable path: %v", err)
	}
	mutexName := filepath.Base(execPath)

	h, err := windows.CreateMutex(nil, false, windows.StringToUTF16Ptr(mutexName))
	if err != nil {
		return fmt.Errorf("failed to create mutex: %v", err)
	}

	if windows.GetLastError() == syscall.ERROR_ALREADY_EXISTS {
		windows.CloseHandle(h)
		return fmt.Errorf("another instance is already running")
	}

	handle = h
	return nil
}

func releaseMutex() {
	if handle != 0 {
		_ = windows.CloseHandle(handle)
	}
}

func isServiceInstalled(name string) (bool, error) {
	cmd := exec.Command("sc.exe", "query", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// If error output indicates non-existence, treat as not installed.
		s := strings.ToLower(string(out))
		if strings.Contains(s, "the specified service does not exist") ||
			strings.Contains(s, "does not exist as an installed service") {
			return false, nil
		}
		return false, fmt.Errorf("sc query: %v: %s", err, string(out))
	}
	return strings.Contains(string(out), "SERVICE_NAME"), nil
}

func scStart(name string) error {
	cmd := exec.Command("sc.exe", "start", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sc start: %v: %s", err, string(out))
	}
	return nil
}

func scStop(name string) error {
	cmd := exec.Command("sc.exe", "stop", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		s := strings.ToLower(string(out))
		if strings.Contains(s, "service has not been started") ||
			strings.Contains(s, "service is not started") {
			return nil
		}
		return fmt.Errorf("sc stop: %v: %s", err, string(out))
	}
	return nil
}

func scDelete(name string) error {
	cmd := exec.Command("sc.exe", "delete", name)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sc delete: %v: %s", err, string(out))
	}
	return nil
}

func installService(agentExePath string) error {
	cmd := exec.Command(agentExePath, "install")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("install service: %v: %s", err, string(out))
	}
	return nil
}

func stopAndKillService(serviceName string) error {
	_ = scStop(serviceName)
	time.Sleep(500 * time.Millisecond)

	// Kill running process, if any
	pid, err := wmiServicePID(serviceName)
	if err != nil {
		// Non-fatal: log and continue
		syslog.L.Error(err).WithMessage("wmic service query failed").Write()
		return nil
	}
	if pid <= 0 {
		return nil
	}

	kill := exec.Command("taskkill", "/PID", fmt.Sprintf("%d", pid), "/F")
	out, err := kill.CombinedOutput()
	if err != nil {
		return fmt.Errorf("taskkill: %v: %s", err, string(out))
	}

	syslog.L.Info().
		WithMessage("Killed service process").
		WithFields(map[string]interface{}{"service": serviceName, "pid": pid}).
		Write()
	return nil
}

func uninstallServiceIfDifferent(serviceName, targetExe string) (bool, error) {
	installed, err := isServiceInstalled(serviceName)
	if err != nil || !installed {
		if err != nil {
			return false, err
		}
		syslog.L.Info().
			WithMessage("Service not found; will install new").
			WithField("service", serviceName).
			Write()
		return false, nil
	}

	currentPath, err := wmiServiceBinaryPath(serviceName)
	if err != nil {
		syslog.L.Error(err).
			WithMessage("cannot read service path via WMI").
			WithField("service", serviceName).
			Write()
		return false, nil
	}

	clean := func(p string) string {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"`)
		p = strings.ToLower(p)
		if idx := strings.Index(strings.ToLower(p), ".exe"); idx >= 0 {
			return p[:idx+4]
		}
		return p
	}

	cur := clean(currentPath)
	tgt := clean(targetExe)

	syslog.L.Info().
		WithMessage("Service binary path check").
		WithFields(map[string]interface{}{
			"service": serviceName,
			"current": cur,
			"target":  tgt,
		}).
		Write()

	if cur != tgt && !strings.HasSuffix(cur, tgt) {
		syslog.L.Info().
			WithMessage("Service installed with different executable path; uninstalling").
			WithField("service", serviceName).
			Write()

		_ = scStop(serviceName)
		time.Sleep(2 * time.Second)
		if err := scDelete(serviceName); err != nil {
			return false, err
		}
		syslog.L.Info().
			WithMessage("Service uninstalled").
			WithField("service", serviceName).
			Write()
		return true, nil
	}

	syslog.L.Info().
		WithMessage("Service already installed at the correct path").
		WithField("service", serviceName).
		Write()
	return false, nil
}

func wmiServicePID(serviceName string) (int, error) {
	cmd := exec.Command(
		"wmic", "service",
		"where", fmt.Sprintf("name='%s'", serviceName),
		"get", "ProcessId", "/value",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("wmic service pid: %v: %s", err, string(out))
	}
	s := string(out)
	var pid int
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "ProcessId=") {
			_, _ = fmt.Sscanf(line, "ProcessId=%d", &pid)
			break
		}
	}
	return pid, nil
}

func wmiServiceBinaryPath(serviceName string) (string, error) {
	cmd := exec.Command(
		"wmic", "service",
		"where", fmt.Sprintf("name='%s'", serviceName),
		"get", "PathName", "/value",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("wmic service pathname: %v: %s", err, string(out))
	}
	s := string(out)
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "PathName=") {
			return strings.TrimPrefix(line, "PathName="), nil
		}
	}
	return "", errors.New("PathName not found")
}
