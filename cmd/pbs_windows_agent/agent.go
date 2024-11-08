//go:build windows
// +build windows

package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/getlantern/systray"
	"github.com/kardianos/service"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/serverlog"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
)

type PingResp struct {
	Pong bool `json:"pong"`
}

//go:embed icon/logo.ico
var icon []byte

type program struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func (p *program) Start(s service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	go p.run(ctx)
	return nil
}

func (p *program) run(ctx context.Context) {
	serverUrl, ok := os.LookupEnv("PBS_AGENT_SERVER")
	if !ok {
		for {
			serverUrl = utils.PromptInput("PBS Agent", "Server URL")

			_, err := url.ParseRequestURI(serverUrl)
			if err == nil {
				serverUrl = strings.TrimSuffix(serverUrl, "/")
				break
			}

			utils.ShowMessageBox("Error", fmt.Sprintf("Invalid server URL: %s", err))
		}

		utils.SetEnvironment("PBS_AGENT_SERVER", serverUrl)
	}
	serverLog, _ := serverlog.InitializeLogger()

	drives := utils.GetLocalDrives()

	defer snapshots.CloseAllSnapshots()

	for _, driveLetter := range drives {
		rune := []rune(driveLetter)[0]
		sftpConfig, _ := sftp.InitializeSFTPConfig(serverUrl, driveLetter)
		if err := sftpConfig.PopulateKeys(); err != nil {
			serverLog.Print(fmt.Sprintf("Unable to populate SFTP keys: %s", err))
			return
		}

		port, err := utils.DriveLetterPort(rune)
		if err != nil {
			serverLog.Print(fmt.Sprintf("Unable to map letter to port: %s", err))
			return
		}

		p.wg.Add(1)
		go func() {
			sftp.Serve(ctx, &p.wg, sftpConfig, "0.0.0.0", port, driveLetter)
		}()
	}

	systray.Run(p.onReady(ctx), p.onExit)
	p.wg.Wait()
}

func (p *program) onReady(ctx context.Context) func() {
	return func() {
		systray.SetIcon(icon)
		systray.SetTitle("Proxmox Backup Agent")
		systray.SetTooltip("Proxmox Backup Agent")

		status := systray.AddMenuItem("Status: Connecting...", "Connectivity status")
		status.Disable()

		go func(ctx context.Context, status *systray.MenuItem) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					var pingResp PingResp
					pingErr := agent.ProxmoxHTTPRequest(http.MethodGet, "/api2/json/ping", nil, &pingResp)
					if pingErr != nil || !pingResp.Pong {
						status.SetTitle(fmt.Sprintf("Status: Error (%s)", pingErr))
						continue
					}
					status.SetTitle("Status: Connected")
				}
			}
		}(ctx, status)

		closeButton := systray.AddMenuItem("Close", "Close PBS Agent")
		go func() {
			<-closeButton.ClickedCh
			systray.Quit()
		}()
	}
}

func (p *program) onExit() {
	systray.Quit()
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	return nil
}

func main() {
	svcConfig := &service.Config{
		Name:        "ProxmoxBackupAgent",
		DisplayName: "Proxmox Backup Agent",
		Description: "Agent for orchestrating backups with Proxmox Backup Server",
		UserName:    "",
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		fmt.Println("Failed to initialize service:", err)
		return
	}

	if len(os.Args) > 1 {
		cmd := os.Args[1]
		switch cmd {
		case "install":
			err = s.Install()
			if err != nil {
				fmt.Println("Failed to install service:", err)
			} else {
				fmt.Println("Service installed")
			}
			return
		case "uninstall":
			err = s.Uninstall()
			if err != nil {
				fmt.Println("Failed to uninstall service:", err)
			} else {
				fmt.Println("Service uninstalled")
			}
			return
		case "start":
			err = s.Start()
			if err != nil {
				fmt.Println("Failed to start service:", err)
			} else {
				fmt.Println("Service started")
			}
			return
		case "stop":
			err = s.Stop()
			if err != nil {
				fmt.Println("Failed to stop service:", err)
			} else {
				fmt.Println("Service stopped")
			}
			return
		default:
			fmt.Println("Unknown command:", cmd)
			fmt.Println("Available commands: install, uninstall, start, stop")
			return
		}
	}

	// If no command provided, run the service as normal
	err = s.Run()
	if err != nil {
		fmt.Println("Error running service:", err)
	}
}
