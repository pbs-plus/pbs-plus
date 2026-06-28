//go:build linux

package tasklog

import (
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

func nodeName() string {
	if h := conf.Env.Hostname; h != "" {
		return proxmox.NormalizeHostname(h)
	}
	h, err := os.Hostname()
	if err != nil {
		return "pbsplus"
	}
	return proxmox.NormalizeHostname(h)
}

func NewTask(node, workerType, wid string) proxmox.Task {
	if node == "" {
		node = nodeName()
	}
	task := proxmox.Task{
		Node:       node,
		PID:        os.Getpid(),
		PStart:     proxmox.GetPStart(),
		StartTime:  time.Now().Unix(),
		WorkerType: workerType,
		WID:        wid,
		User:       proxmox.AuthID,
	}
	pidHex := fmt.Sprintf("%08X", task.PID)
	pstartHex := fmt.Sprintf("%08X", task.PStart)
	startHex := fmt.Sprintf("%08X", uint32(task.StartTime))
	taskID := fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:",
		task.Node, pidHex, pstartHex, taskID, startHex, task.WorkerType, task.WID, task.User)
	return task
}
