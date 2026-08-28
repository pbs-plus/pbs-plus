//go:build linux

package tasklog

import (
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
)

func FormatWorkerID(store, prefix, identifier string) string {
	return proxmox.EncodeToHexEscapes(store) +
		proxmox.EncodeToHexEscapes(":") +
		prefix +
		proxmox.EncodeToHexEscapes(identifier)
}

func IsQueuedUPID(upid string) bool {
	task, err := proxmox.ParseUPID(upid)
	return err == nil && task.Node == "pbsplusgen-queue"
}

func NewTask(node, workerType, wid string) proxmox.Task {
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
	task.TaskId = fmt.Sprintf("%08X", rand.Uint32())
	task.UPID = fmt.Sprintf("UPID:%s:%s:%s:%s:%s:%s:%s:%s:",
		task.Node, pidHex, pstartHex, task.TaskId, startHex, task.WorkerType, task.WID, task.User)
	return task
}
