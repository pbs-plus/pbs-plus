package system

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreos/go-systemd/v22/dbus"
	godbus "github.com/godbus/dbus/v5"
)

func GenerateMountServiceName(datastore, ns, backupType, backupID, safeTime string) string {
	parts := []string{"pbs-plus-restore", datastore}
	if ns != "" {
		parts = append(parts, strings.ReplaceAll(ns, "/", "-"))
	}
	parts = append(parts, backupType, backupID, safeTime)

	name := strings.Join(parts, "-")
	name = strings.ReplaceAll(name, " ", "-")
	name = strings.ReplaceAll(name, ":", "-")

	return dbus.PathBusEscape(name) + ".service"
}

func CreateMountService(ctx context.Context, serviceName, mountPoint string, args []string) error {
	conn, err := getConn()
	if err != nil {
		return err
	}

	execStart := append([]string{"/usr/bin/pxar-direct-mount"}, args...)

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
			Value: godbus.MakeVariant("inactive"), // Equivalent to --collect
		},
	}

	_, err = conn.StartTransientUnitContext(ctx, serviceName, "replace", props, nil)
	if err != nil {
		return fmt.Errorf("failed to start transient service %s: %w", serviceName, err)
	}

	return nil
}

func StopMountService(ctx context.Context, serviceName string) error {
	conn, err := getConn()
	if err != nil {
		return err
	}

	done := make(chan string)
	if _, err := conn.StopUnitContext(ctx, serviceName, "replace", done); err == nil {
		<-done
	}

	_ = conn.ResetFailedUnitContext(ctx, serviceName)

	return nil
}

func ListMountServices(ctx context.Context) ([]string, error) {
	conn, err := getConn()
	if err != nil {
		return nil, err
	}

	units, err := conn.ListUnitsByPatternsContext(ctx, nil, []string{"pbs-plus-restore-*.service"})
	if err != nil {
		return []string{}, nil
	}

	services := make([]string, 0, len(units))
	for _, unit := range units {
		services = append(services, unit.Name)
	}

	return services, nil
}
