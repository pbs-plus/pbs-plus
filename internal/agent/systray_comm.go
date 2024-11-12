//go:build windows

package agent

import (
	"golang.org/x/sys/windows/registry"
)

func SetStatus(status string) error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.ALL_ACCESS)
	if err == nil {
		defer key.Close()
		err := key.SetStringValue("Status", status)
		return err
	}
	return err
}

func GetStatus() (string, error) {
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus`, registry.QUERY_VALUE)
	if err == nil {
		defer key.Close()
		regStatus, _, err := key.GetStringValue("Status")
		return regStatus, err
	}

	return "", err
}
