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
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
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
	logger, err := syslog.InitializeLogger(p.svc)
	if err != nil {
		utils.ShowMessageBox("Error", fmt.Sprintf("Failed to initialize logger: %s", err))
	}

	var serverUrl string
	for {
		serverUrl = utils.PromptInput("PBS Plus Agent", "Server URL")

		_, err := url.ParseRequestURI(serverUrl)
		if err == nil {
			serverUrl = strings.TrimSuffix(serverUrl, "/")
			break
		}

		logger.Errorf("Invalid server URL: %s", err)
	}

	return serverUrl
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
		logger, err := syslog.InitializeLogger(p.svc)
		if err != nil {
			utils.ShowMessageBox("Error", fmt.Sprintf("Failed to initialize logger: %s", err))
		}

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
				retryWait := utils.WaitChan(time.Second * 2)
				select {
				case <-ctx.Done():
					return
				case <-retryWait:
					setIP()
				}
			}
		}(p.ctx, serverIP, &url)

		status := systray.AddMenuItem("Status: Connecting...", "Connectivity status")
		status.Disable()

		go func(ctx context.Context, status *systray.MenuItem, url *string) {
			setStatus := func() {
				if url != nil && *url != "" {
					svcStatus, ok := os.LookupEnv("PBS_AGENT_STATUS")
					if !ok {
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
				retryWait := utils.WaitChan(time.Second * 2)
				select {
				case <-ctx.Done():
					return
				case <-retryWait:
					setStatus()
				}
			}
		}(p.ctx, status, &url)

		systray.AddSeparator()

		changeUrl := systray.AddMenuItem("Change Server", "Change Server URL")

		go func(ctx context.Context, changeUrl *systray.MenuItem) {
			if err != nil {
				utils.ShowMessageBox("Error", fmt.Sprintf("Failed to initialize logger: %s", err))
			}

			for {
				select {
				case <-ctx.Done():
					return
				case <-changeUrl.ClickedCh:
					serverUrl := p.askServerUrl()

					if !isAdmin() {
						err := runAsAdminServerUrl(serverUrl)
						if err != nil {
							logger.Errorf("Failed to run as administrator: %s", err)
							continue
						}
					} else {
						err = setServerURLAdmin(serverUrl)
						if err != nil {
							logger.Errorf("Failed to set new server URL: %s", err)
							continue
						}
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

func setServerURLAdmin(serverUrl string) error {
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE, `Software\PBSPlus\Config`, registry.ALL_ACCESS)
	if err != nil {
		return fmt.Errorf("setServerURLAdmin: error creating HKLM key -> %w", err)
	}
	defer key.Close()

	if err := key.SetStringValue("ServerURL", serverUrl); err != nil {
		return fmt.Errorf("setServerURLAdmin: error setting HKLM value -> %w", err)
	}

	return nil
}

func runAsAdminServerUrl(serverUrl string) error {
	verb := "runas"
	exe, _ := os.Executable()
	cwd, _ := os.Getwd()
	args := fmt.Sprintf("--set-server-url %s", serverUrl) // Append the server URL to the arguments

	verbPtr, _ := syscall.UTF16PtrFromString(verb)
	exePtr, _ := syscall.UTF16PtrFromString(exe)
	cwdPtr, _ := syscall.UTF16PtrFromString(cwd)
	argPtr, _ := syscall.UTF16PtrFromString(args)

	var showCmd int32 = 1 //SW_NORMAL

	err := windows.ShellExecute(0, verbPtr, exePtr, argPtr, cwdPtr, showCmd)
	if err != nil {
		return fmt.Errorf("Failed to run as administrator: %s", err)
	}

	return nil
}
