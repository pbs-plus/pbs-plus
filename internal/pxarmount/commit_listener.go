package pxarmount

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/pbstoken"
)

type CommitRequest struct {
	PBSURL     string
	Datastore  string
	AuthToken  string
	Namespace  string
	BackupID   string
	BackupType string
	SkipTLS    bool
}

func ParseCommitLine(line string) (*CommitRequest, error) {
	parts := strings.SplitN(line, " ", 7)
	if len(parts) < 1 || parts[0] != "COMMIT" {
		return nil, fmt.Errorf("invalid COMMIT format")
	}
	req := &CommitRequest{}
	if len(parts) > 1 {
		req.PBSURL = parts[1]
	}
	if len(parts) > 2 {
		req.Datastore = parts[2]
	}
	if len(parts) > 3 {
		req.AuthToken = parts[3]
	}
	if len(parts) > 4 && parts[4] != "-" {
		req.Namespace = parts[4]
	}
	if len(parts) > 5 {
		req.BackupType = parts[5]
	}
	if len(parts) > 6 {
		req.BackupID = parts[6]
	}
	if req.BackupType == "" {
		req.BackupType = "host"
	}
	return req, nil
}

func ReadLocalToken() string {
	return pbstoken.ReadLocal()
}

func ResolveDatastoreName(pbsStore string) string {
	out, err := exec.Command("proxmox-backup-manager", "datastore", "list", "--output-format", "json").Output()
	if err != nil {
		return filepath.Base(pbsStore)
	}
	var dss []struct {
		Name string `json:"name"`
		Path string `json:"path"`
	}
	if err := json.Unmarshal(out, &dss); err != nil {
		return filepath.Base(pbsStore)
	}
	cleanPath := filepath.Clean(pbsStore)
	for _, ds := range dss {
		if filepath.Clean(ds.Path) == cleanPath {
			return ds.Name
		}
	}
	return filepath.Base(pbsStore)
}

func StartCommitListener(sockPath string, mfs *MutableFS) (net.Listener, error) {
	_ = os.Remove(sockPath)
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sockPath, 0o660); err != nil {
		_ = l.Close()
		return nil, err
	}

	hub, err := newCommitHub(sockPath, mfs.verbose)
	if err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("start monitor hub: %w", err)
	}
	globalCommitHub = hub

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go handleCommitConn(mfs, conn)
		}
	}()
	return l, nil
}

func handleCommitConn(mfs *MutableFS, conn net.Conn) {
	defer func() { _ = conn.Close() }()
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		return
	}
	line := scanner.Text()
	req, err := ParseCommitLine(line)
	if err != nil {
		_, _ = fmt.Fprintf(conn, "ERR %v\n", err)
		return
	}

	detached := false
	_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	if scanner.Scan() {
		detached = scanner.Text() == "DETACH"
	}
	_ = conn.SetReadDeadline(time.Time{})

	if detached {
		jobID := globalCommitHub.startJob()
		_, _ = fmt.Fprintf(conn, "JOB %d\n", jobID)
		_ = conn.Close()

		go func() {
			defer globalCommitHub.endJob()
			prog := newHubProgressReporter()
			if err := CommitSnapshot(mfs, req, prog); err != nil {
				prog.Error(err.Error())
				return
			}
		}()
	} else {
		globalCommitHub.startJob()
		defer globalCommitHub.endJob()

		primary := NewProgressReporter(conn)
		prog := &fanoutReporter{primary: primary, hub: globalCommitHub, started: time.Now()}
		if err := CommitSnapshot(mfs, req, prog); err != nil {
			prog.Error(err.Error())
			return
		}
	}
}
