//go:build windows
// +build windows

package main

import (
	"context"
	"fmt"
	"time"

	_ "embed"

	"github.com/getlantern/systray"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-plus/internal/agent"
	"golang.org/x/sys/windows/registry"
)

//go:embed icon/logo.ico
var icon []byte

type agentTray struct {
	ctx context.Context
	svc service.Service
}

func (p *agentTray) foregroundTray() error {
	var serverUrl string

	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.QUERY_VALUE)
	if err == nil {
		defer key.Close()
		serverUrl, _, _ = key.GetStringValue("ServerURL")
	}

	systray.Run(p.onReady(serverUrl), p.onExit)

	return nil
}

func (p *agentTray) onReady(url string) func() {
	return func() {
		systray.SetIcon(icon)
		systray.SetTitle("PBS Plus Agent")
		systray.SetTooltip("PBS Plus Agent")

		serverIP := systray.AddMenuItem(fmt.Sprintf("Server: %s", url), "PBS Plus overlay address")
		serverIP.Disable()

		go func(ctx context.Context, serverIP *systray.MenuItem, url *string) {
			setIP := func() {
				if url != nil && *url != "" {
					serverIP.SetTitle(fmt.Sprintf("Server: %s", *url))
				} else {
					serverIP.SetTitle("Server: N/A")
				}
			}

			setIP()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second * 2):
					setIP()
				}
			}
		}(p.ctx, serverIP, &url)

		status := systray.AddMenuItem("Status: Connecting...", "Connectivity status")
		status.Disable()

		go func(ctx context.Context, status *systray.MenuItem, url *string) {
			setStatus := func() {
				if url != nil && *url != "" {
					svcStatus, err := agent.GetStatus()
					if err != nil {
						status.SetTitle("Status: Agent service not running")
					} else {
						status.SetTitle(fmt.Sprintf("Status: %s", svcStatus))
					}
				} else {
					status.SetTitle("Status: Server URL needs to be set.")
				}
			}
			setStatus()

			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second * 2):
					setStatus()
				}
			}
		}(p.ctx, status, &url)

		systray.AddSeparator()

		closeButton := systray.AddMenuItem("Close", "Close tray icon")
		go func(ctx context.Context, closeButton *systray.MenuItem) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-closeButton.ClickedCh:
					systray.Quit()
				}
			}
		}(p.ctx, closeButton)
	}
}

func (p *agentTray) onExit() {
}
