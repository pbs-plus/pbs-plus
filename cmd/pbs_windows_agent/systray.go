//go:build windows
// +build windows

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	_ "embed"

	"github.com/getlantern/systray"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/serverlog"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
)

//go:embed icon/logo.ico
var icon []byte

type agentTray struct {
	ctx context.Context
	svc service.Service
}

func (p *agentTray) askServerUrl() string {
	var serverUrl string
	for {
		serverUrl = utils.PromptInput("PBS Agent", "Server URL")

		_, err := url.ParseRequestURI(serverUrl)
		if err == nil {
			serverUrl = strings.TrimSuffix(serverUrl, "/")
			break
		}

		utils.ShowMessageBox("Error", fmt.Sprintf("Invalid server URL: %s", err))
	}

	return serverUrl
}

func (p *agentTray) foregroundTray() error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\ProxmoxAgent\Config`, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("foregroundTray: error creating HKLM key -> %w", err)
	}
	defer key.Close()

	var serverUrl string
	if serverUrl, _, err = key.GetStringValue("ServerURL"); err != nil || serverUrl == "" {
		serverUrl = p.askServerUrl()
		if err := key.SetStringValue("ServerURL", serverUrl); err != nil {
			return fmt.Errorf("foregroundTray: error setting HKLM value -> %w", err)
		}
	}

	systray.Run(p.onReady(serverUrl), p.onExit)

	return nil
}

func (p *agentTray) onReady(url string) func() {
	return func() {
		p.svc.Start()

		systray.SetIcon(icon)
		systray.SetTitle("Proxmox Backup Agent")
		systray.SetTooltip("Proxmox Backup Agent")

		serverIP := systray.AddMenuItem(fmt.Sprintf("Server: %s", url), "Proxmox Backup Server address")
		serverIP.Disable()

		status := systray.AddMenuItem("Status: Connecting...", "Connectivity status")
		status.Disable()

		go func(ctx context.Context, status *systray.MenuItem) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					svcStatus, ok := os.LookupEnv("PBS_AGENT_STATUS")
					if !ok {
						status.SetTitle("Status: Agent service not running")
						continue
					}
					status.SetTitle(fmt.Sprintf("Status: %s", svcStatus))

					time.Sleep(time.Second)
				}
			}
		}(p.ctx, status)

		systray.AddSeparator()

		changeUrl := systray.AddMenuItem("Change Server", "Change Server URL")
		go func(ctx context.Context, changeUrl *systray.MenuItem) {
			key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\ProxmoxAgent\Config`, registry.ALL_ACCESS)
			if err != nil {
				utils.ShowMessageBox("Error", fmt.Sprintf("Failed to retrieve registry key: %s", err))
				changeUrl.Hide()
				return
			}
			defer key.Close()

			for {
				select {
				case <-ctx.Done():
					return
				case <-changeUrl.ClickedCh:
					serverUrl := p.askServerUrl()
					if err := key.SetStringValue("ServerURL", serverUrl); err != nil {
						utils.ShowMessageBox("Error", fmt.Sprintf("Failed to set new server url: %s", err))
						continue
					}

					err = p.svc.Stop()
					if err != nil {
						utils.ShowMessageBox("Error", fmt.Sprintf("Failed to restart service: %s", err))
						continue
					}

					err = p.svc.Start()
					if err != nil {
						utils.ShowMessageBox("Error", fmt.Sprintf("Failed to restart service: %s", err))
						continue
					}
				}
			}
		}(p.ctx, changeUrl)

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

func isAdmin() bool {
	_, err := os.Open("\\\\.\\PHYSICALDRIVE0")
	if err != nil {
		return false
	}
	return true
}

func runAsAdmin() {
	serverLog, _ := serverlog.InitializeLogger()
	verb := "runas"
	exe, _ := os.Executable()
	cwd, _ := os.Getwd()
	args := strings.Join(os.Args[1:], " ")

	verbPtr, _ := syscall.UTF16PtrFromString(verb)
	exePtr, _ := syscall.UTF16PtrFromString(exe)
	cwdPtr, _ := syscall.UTF16PtrFromString(cwd)
	argPtr, _ := syscall.UTF16PtrFromString(args)

	var showCmd int32 = 1 //SW_NORMAL

	err := windows.ShellExecute(0, verbPtr, exePtr, argPtr, cwdPtr, showCmd)
	if err != nil {
		if serverLog != nil {
			serverLog.Print(fmt.Sprintf("Failed to run as administrator: %s", err))
		}
		utils.ShowMessageBox("Error", fmt.Sprintf("Failed to run as administrator: %s", err))
	}
}
