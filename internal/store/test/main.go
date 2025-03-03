package main

import (
	"log"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/proxmox"
)

func main() {
	task, err := proxmox.ParseUPID("UPID:phoenix-pbs:000BA81C:05228D40:00000034:67C3639C:backup:phoenix\\x3ahost-DP\\x2dD001:root@pam!pbs-plus-auth:")
	if err != nil {
		log.Fatal(err)
	}

	log.Println(task)
}
