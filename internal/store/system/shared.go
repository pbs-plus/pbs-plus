package system

import (
	"context"
	"fmt"
	"sync"

	"github.com/coreos/go-systemd/v22/dbus"
)

var (
	sharedConn *dbus.Conn
	connMutex  sync.Mutex
)

func getConn() (*dbus.Conn, error) {
	connMutex.Lock()
	defer connMutex.Unlock()

	if sharedConn != nil {
		_, err := sharedConn.GetManagerProperty("Version")
		if err == nil {
			return sharedConn, nil
		}
		sharedConn.Close()
		sharedConn = nil
	}

	conn, err := dbus.NewSystemdConnectionContext(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to systemd bus: %w", err)
	}

	sharedConn = conn
	return sharedConn, nil
}
