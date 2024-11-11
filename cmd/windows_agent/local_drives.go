//go:build windows

package main

import (
	"fmt"
	"os"

	"github.com/sonroyaalmerol/pbs-plus/internal/agent/sftp"
	"github.com/sonroyaalmerol/pbs-plus/internal/utils"
)

type Drive struct {
	Letter    string
	ErrorChan chan string
}

func getLocalDrives() (r []Drive) {
	for _, drive := range "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {
		f, err := os.Open(string(drive) + ":\\")
		if err == nil {
			r = append(r, Drive{Letter: string(drive)})
			f.Close()
		}
	}
	return
}

func (drive *Drive) serveSFTP(p *agentService) error {
	rune := []rune(drive.Letter)[0]
	sftpConfig, err := sftp.InitializeSFTPConfig(p.svc, drive.Letter)
	if err != nil {
		return fmt.Errorf("Unable to initialize SFTP config: %s", err)
	}
	if err := sftpConfig.PopulateKeys(); err != nil {
		return fmt.Errorf("Unable to populate SFTP keys: %s", err)
	}

	port, err := utils.DriveLetterPort(rune)
	if err != nil {
		return fmt.Errorf("Unable to map letter to port: %s", err)
	}

	go sftp.Serve(p.ctx, drive.ErrorChan, sftpConfig, "0.0.0.0", port, drive.Letter)

	return nil
}
