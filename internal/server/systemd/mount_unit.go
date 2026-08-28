package systemd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

const mountProcessDir = "/var/run/pbs-plus-mounts"

func mountPidFile(serviceName string) string {
	return filepath.Join(mountProcessDir, serviceName+".pid")
}

func startMountProcess(serviceName string, args []string) error {
	if err := os.MkdirAll(mountProcessDir, 0o755); err != nil {
		return fmt.Errorf("create mount process dir: %w", err)
	}

	logFile, err := os.OpenFile(filepath.Join(mountProcessDir, serviceName+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open mount process log: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command("/usr/bin/pxar-mount", args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s as process: %w", serviceName, err)
	}

	pid := cmd.Process.Pid
	go func() {
		_ = cmd.Wait()
		if data, err := os.ReadFile(mountPidFile(serviceName)); err == nil && strings.TrimSpace(string(data)) == fmt.Sprintf("%d", pid) {
			_ = os.Remove(mountPidFile(serviceName))
		}
	}()

	if err := os.WriteFile(mountPidFile(serviceName), []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
		return fmt.Errorf("write mount pidfile: %w", err)
	}
	return nil
}

func stopMountProcess(serviceName string) error {
	data, err := os.ReadFile(mountPidFile(serviceName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil || pid <= 0 {
		_ = os.Remove(mountPidFile(serviceName))
		return nil
	}

	_ = syscall.Kill(-pid, syscall.SIGTERM)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(pid, 0) != nil {
			_ = os.Remove(mountPidFile(serviceName))
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	_ = syscall.Kill(-pid, syscall.SIGKILL)
	_ = os.Remove(mountPidFile(serviceName))
	return nil
}

func listMountProcesses() ([]string, error) {
	entries, err := os.ReadDir(mountProcessDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}
	services := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".pid") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(mountProcessDir, entry.Name()))
		if err != nil {
			continue
		}
		var pid int
		if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil || pid <= 0 {
			continue
		}
		if syscall.Kill(pid, 0) != nil {
			continue
		}
		services = append(services, strings.TrimSuffix(entry.Name(), ".pid"))
	}
	return services, nil
}

func MountServiceKey(datastore, ns, backupType, backupID, safeTime string) string {
	rawID := fmt.Sprintf("%s|%s|%s|%s|%s", datastore, ns, backupType, backupID, safeTime)

	shortHash := crypto.SHA256Hex([]byte(rawID))[:16]

	safeDs := strings.ReplaceAll(datastore, "/", "-")
	if len(safeDs) > 20 {
		safeDs = safeDs[:20]
	}

	return fmt.Sprintf("%s-%s", safeDs, shortHash)
}

func GenerateMountServiceName(datastore, ns, backupType, backupID, safeTime string) string {
	name := fmt.Sprintf("pbs-plus-snapshot-mount-%s", MountServiceKey(datastore, ns, backupType, backupID, safeTime))

	return name + ".service"
}

func CreateMountService(ctx context.Context, serviceName, mountPoint string, args []string) error {
	conn, err := Conn()
	if err != nil {
		return startMountProcess(serviceName, args)
	}

	execStart := append([]string{"/usr/bin/pxar-mount"}, args...)

	props := []dbus.Property{
		dbus.PropDescription("PBS Plus restore mount for " + mountPoint),
		dbus.PropExecStart(execStart, false),
		{
			Name:  "RemainAfterExit",
			Value: godbus.MakeVariant(true),
		},
		{
			Name:  "Type",
			Value: godbus.MakeVariant("simple"),
		},
		{
			Name:  "KillMode",
			Value: godbus.MakeVariant("control-group"),
		},
		{
			Name:  "Restart",
			Value: godbus.MakeVariant("no"),
		},
		{
			Name:  "CollectMode",
			Value: godbus.MakeVariant("inactive"),
		},
	}

	_, err = conn.StartTransientUnitContext(ctx, serviceName, "replace", props, nil)
	if err != nil {
		return fmt.Errorf("failed to start transient service %s: %w", serviceName, err)
	}

	return nil
}

func StopMountService(ctx context.Context, serviceName string) error {
	conn, err := Conn()
	if err != nil {
		return stopMountProcess(serviceName)
	}

	done := make(chan string)
	if _, err := conn.StopUnitContext(ctx, serviceName, "replace", done); err == nil {
		<-done
	}

	if err := conn.ResetFailedUnitContext(ctx, serviceName); err != nil {
		log.Error(err, "")
	}

	return nil
}

func ListMountServices(ctx context.Context) ([]string, error) {
	conn, err := Conn()
	if err != nil {
		return listMountProcesses()
	}

	units, err := conn.ListUnitsByPatternsContext(ctx, nil, []string{"pbs-plus-snapshot-mount-*.service"})
	if err != nil {
		return []string{}, nil
	}

	services := make([]string, 0, len(units))
	for _, unit := range units {
		services = append(services, unit.Name)
	}

	return services, nil
}
