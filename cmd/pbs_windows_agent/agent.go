//go:build windows
// +build windows

package main

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/windows"

	"github.com/getlantern/systray"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/serverlog"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/agent/snapshots"
	"github.com/sonroyaalmerol/pbs-d2d-backup/internal/utils"
)

//go:embed icon/logo.ico
var icon []byte

func main() {
	if !isAdmin() {
		fmt.Println("This program needs to be run as administrator.")
		runAsAdmin()
		os.Exit(0)
	}

	serverUrl, ok := os.LookupEnv("PBS_AGENT_SERVER")
	if !ok {
		for {
			serverUrl = utils.PromptInput("PBS Agent", "Server URL")

			_, err := url.ParseRequestURI(serverUrl)
			if err == nil {
				serverUrl = strings.TrimSuffix(serverUrl, "/")
				break
			}
		}

		utils.SetEnvironment("PBS_AGENT_SERVER", serverUrl)
	}

	_, err := url.ParseRequestURI(serverUrl)
	if err != nil {
		utils.ShowMessageBox("Error", fmt.Sprintf("Invalid server URL: %s", err))
		os.Exit(1)
	}

	serverLog, _ := serverlog.InitializeLogger()

	// Reserve port 33450-33476
	drives := utils.GetLocalDrives()
	ctx := context.Background()

	var wg sync.WaitGroup
	for _, driveLetter := range drives {
		rune := []rune(driveLetter)[0]

		sftpConfig, _ := sftp.InitializeSFTPConfig(serverUrl, driveLetter)

		err = sftpConfig.PopulateKeys()
		if err != nil {
			if serverLog != nil {
				serverLog.Print(fmt.Sprintf("Unable to populate SFTP keys: %s", err))
			}
			utils.ShowMessageBox("Error", fmt.Sprintf("Unable to populate SFTP keys: %s", err))
			os.Exit(1)
		}

		port, err := utils.DriveLetterPort(rune)
		if err != nil {
			if serverLog != nil {
				serverLog.Print(fmt.Sprintf("Unable to map letter to port: %s", err))
			}
			utils.ShowMessageBox("Error", fmt.Sprintf("Unable to map letter to port: %s", err))
			os.Exit(1)
		}

		wg.Add(1)
		go sftp.Serve(ctx, &wg, sftpConfig, "0.0.0.0", port, driveLetter)
	}

	defer snapshots.CloseAllSnapshots()

	systray.Run(onReady(serverUrl), onExit)
	defer systray.Quit()

	wg.Wait()
}

func onReady(serverUrl string) func() {
	serverLog, _ := serverlog.InitializeLogger()

	return func() {
		systray.SetIcon(icon)
		systray.SetTitle("Proxmox Backup Agent")
		systray.SetTooltip("Orchestrating backups with Proxmox Backup Server")

		url, err := url.Parse(serverUrl)
		if err != nil {
			if serverLog != nil {
				serverLog.Print(fmt.Sprintf("Failed to parse server URL: %s", err))
			}
			utils.ShowMessageBox("Error", fmt.Sprintf("Failed to parse server URL: %s", err))
			os.Exit(1)
		}

		serverIP := systray.AddMenuItem(fmt.Sprintf("Server: %s", url), "Proxmox Backup Server address")
		serverIP.Disable()
	}
}

func onExit() {
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
