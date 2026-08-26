//go:build linux

package tasklog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"

	"github.com/pbs-plus/pbs-plus/internal/proxmox"
	"golang.org/x/sys/unix"
)

// PBS's proxmox-daemon control socket: an abstract unix socket at
// "@/run/proxmox-backup/control-<pid>.sock". proxmox-backup derives the
// same path from a task UPID's pid and sends worker-task-status /
// worker-task-abort commands, so serving it is what lets the real PBS
// daemon (and its UI) query and stop pbs-plus tasks while they run.
const controlSocketDir = "/run/proxmox-backup"

var controlSocketOnce sync.Once

func controlSocketPath() string {
	return "@" + controlSocketDir + "/control-" + strconv.Itoa(os.Getpid()) + ".sock"
}

func startControlSocket() {
	controlSocketOnce.Do(func() {
		ln, err := net.Listen("unix", controlSocketPath())
		if err != nil {
			slog.Error("tasklog: control socket", "error", err)
			return
		}
		go serveControlSocket(ln)
	})
}

func writeReply(conn net.Conn, format string, args ...any) {
	if _, err := fmt.Fprintf(conn, format, args...); err != nil {
		slog.Error("tasklog: control socket write", "error", err)
	}
}

func serveControlSocket(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			slog.Error("tasklog: control socket accept", "error", err)
			return
		}
		go handleControlConn(conn)
	}
}

func handleControlConn(conn net.Conn) {
	defer func() {
		if cerr := conn.Close(); cerr != nil {
			slog.Error(cerr.Error())
		}
	}()

	if !peerAllowed(conn) {
		writeReply(conn, "ERROR: permission denied\n")
		return
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var req struct {
			Command string          `json:"command"`
			Args    json.RawMessage `json:"args"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &req); err != nil {
			writeReply(conn, "ERROR: %v\n", err)
			continue
		}
		result, err := dispatchControlCommand(req.Command, req.Args)
		if err != nil {
			writeReply(conn, "ERROR: %v\n", err)
			continue
		}
		reply, err := json.Marshal(result)
		if err != nil {
			writeReply(conn, "ERROR: %v\n", err)
			continue
		}
		writeReply(conn, "OK: %s\n", reply)
	}
}

// peerAllowed mirrors PBS's PeerCredentials gate: only root and the
// backup user (uid/gid 34) may talk to the control socket.
func peerAllowed(conn net.Conn) bool {
	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return false
	}
	rc, err := uc.SyscallConn()
	if err != nil {
		return false
	}
	var allowed bool
	rcErr := rc.Control(func(fd uintptr) {
		cred, err := unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if err != nil {
			return
		}
		allowed = cred.Uid == 0 || cred.Uid == backupUID || cred.Gid == backupGID ||
			cred.Uid == uint32(os.Geteuid())
	})
	return rcErr == nil && allowed
}

func dispatchControlCommand(command string, args json.RawMessage) (any, error) {
	var a struct {
		UPID string `json:"upid"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("tasklog: bad args: %w", err)
	}

	task, err := proxmox.ParseUPID(a.UPID)
	if err != nil {
		return nil, fmt.Errorf("tasklog: parse upid: %w", err)
	}
	if !upidIsLocal(task) {
		return nil, fmt.Errorf("upid does not belong to this process")
	}

	switch command {
	case "worker-task-status":
		_, active := lookupWorker(task.TaskId)
		return active, nil
	case "worker-task-abort":
		if wt, ok := lookupWorker(task.TaskId); ok {
			wt.RequestAbort()
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown command %q", command)
	}
}

func upidIsLocal(task proxmox.Task) bool {
	p, err := selfPStart()
	return err == nil && task.PID == os.Getpid() && task.PStart == p
}
