//go:build linux

package proxmox

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/conf"
	"log/slog"
)

func NormalizeHostname(hostname string) string {
	return strings.ReplaceAll(strings.ReplaceAll(hostname, ".", "-"), " ", "-")
}

func ParseUPID(upid string) (Task, error) {
	pattern := `^UPID:(?P<node>[a-zA-Z0-9]([a-zA-Z0-9\-]*[a-zA-Z0-9])?):(?P<pid>[0-9A-Fa-f]{8}):(?P<pstart>[0-9A-Fa-f]{8,9}):(?P<task_id>[0-9A-Fa-f]{8,16}):(?P<starttime>[0-9A-Fa-f]{8}):(?P<wtype>[^:\s]+):(?P<wid>[^:\s]*):(?P<authid>[^:\s]+):$`

	re, err := regexp.Compile(pattern)
	if err != nil {
		return Task{}, fmt.Errorf("failed to compile regex: %w", err)
	}

	matches := re.FindStringSubmatch(upid)
	if matches == nil {
		return Task{}, fmt.Errorf("invalid UPID format")
	}

	task := Task{UPID: upid}
	for i, name := range re.SubexpNames() {
		if name == "" || i >= len(matches) {
			continue
		}
		switch name {
		case "node":
			task.Node = matches[i]
		case "pid":
			pid, err := strconv.ParseInt(matches[i], 16, 32)
			if err != nil {
				return Task{}, fmt.Errorf("failed to parse PID: %w", err)
			}
			task.PID = int(pid)
		case "pstart":
			pstart, err := strconv.ParseInt(matches[i], 16, 32)
			if err != nil {
				return Task{}, fmt.Errorf("failed to parse PStart: %w", err)
			}
			task.PStart = uint64(pstart)
		case "starttime":
			startTime, err := strconv.ParseInt(matches[i], 16, 64)
			if err != nil {
				return Task{}, fmt.Errorf("failed to parse StartTime: %w", err)
			}
			task.StartTime = startTime
		case "wtype":
			task.WorkerType = matches[i]
		case "task_id":
			task.TaskId = matches[i]
		case "wid":
			task.WID = matches[i]
		case "authid":
			task.User = matches[i]
		}
	}

	return task, nil
}

func (task *Task) GenerateUPID() string {
	upid := fmt.Sprintf(
		"UPID:%s:%08X:%08X:%s:%08X:%s:%s:%s:",
		task.Node,
		task.PID,
		task.PStart,
		task.TaskId,
		task.StartTime,
		task.WorkerType,
		task.WID,
		task.User,
	)
	return upid
}

var pstart = atomic.Int32{}

func GetPStart() uint64 {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return uint64(pstart.Add(1))
	}

	fields := strings.Fields(string(data))
	if len(fields) < 22 {
		return uint64(pstart.Add(1))
	}

	pstartA, err := strconv.ParseUint(fields[21], 10, 64)
	if err != nil {
		return uint64(pstart.Add(1))
	}
	return pstartA
}

func ChangeUPIDStartTime(upid string, startTime time.Time) (string, error) {
	if !strings.HasPrefix(upid, "UPID:") || !strings.HasSuffix(upid, ":") {
		return "", fmt.Errorf("invalid UPID format: must start with 'UPID:' and end with ':'")
	}

	parsedTask, err := ParseUPID(upid)
	if err != nil {
		return "", err
	}

	logFolder := fmt.Sprintf("%02X", parsedTask.PStart&0xFF)
	path := filepath.Join(conf.TaskLogsBasePath, logFolder, upid)

	parsedTask.StartTime = startTime.Unix()

	newUpid := parsedTask.GenerateUPID()

	newLogFolder := fmt.Sprintf("%02X", parsedTask.PStart&0xFF)
	newPath := filepath.Join(conf.TaskLogsBasePath, newLogFolder, newUpid)

	err = os.Rename(path, newPath)
	if err != nil {
		return "", err
	}
	slog.Info("updated UPID start time")

	if err := os.Symlink(newPath, path); err != nil {
		slog.Error(err.Error())
	}

	return newUpid, nil
}

func EncodeToHexEscapes(input string) string {
	var encoded strings.Builder
	for _, char := range input {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' {
			encoded.WriteRune(char)
		} else {
			encoded.WriteString(fmt.Sprintf(`\x%02x`, char))
		}
	}

	return encoded.String()
}

func buildGroupPath(ns, backupType, backupID, backupTime string) string {
	var parts []string

	if ns != "" {
		nsParts := strings.SplitSeq(ns, "/")
		for nsPart := range nsParts {
			if nsPart != "" {
				parts = append(parts, "ns", nsPart)
			}
		}
	}

	parts = append(parts, backupType, backupID, backupTime)

	return filepath.Join(parts...)
}

// BuildPxarPaths returns filesystem paths for pxar/mpxar/ppxar files.
// For split pxar both paths are populated; for non-split, ppxarPath is empty.
func BuildPxarPaths(pbsStoreRoot, ns, backupType, backupID, backupTime, fileName string) (mpxarPath, ppxarPath string, isMetadataSplit bool, err error) {
	groupPath := buildGroupPath(ns, backupType, backupID, backupTime)
	groupDir := filepath.Join(pbsStoreRoot, groupPath)

	if fileName == "" {
		entries, err := os.ReadDir(groupDir)
		if err != nil {
			return "", "", false, fmt.Errorf("failed to read backup directory: %w", err)
		}

		// Prefer split archives (.mpxar.didx), then .pxar.didx.
		var foundMpxar, foundPxar string
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name := entry.Name()
			if strings.HasSuffix(name, ".mpxar.didx") && foundMpxar == "" {
				foundMpxar = name
			} else if strings.HasSuffix(name, ".pxar.didx") && foundPxar == "" {
				foundPxar = name
			}
		}

		if foundMpxar != "" {
			fileName = foundMpxar
		} else if foundPxar != "" {
			fileName = foundPxar
		} else {
			return "", "", false, fmt.Errorf("no .pxar.didx or .mpxar.didx file found in %s", groupDir)
		}
	}

	if !strings.HasSuffix(fileName, ".mpxar.didx") &&
		!strings.HasSuffix(fileName, ".pxar.didx") &&
		!strings.HasSuffix(fileName, ".ppxar.didx") {
		return "", "", false, fmt.Errorf("file-name must end with .pxar.didx, .mpxar.didx, or .ppxar.didx")
	}

	switch {
	case strings.HasSuffix(fileName, ".pxar.didx"):
		mpxarPath = filepath.Join(groupDir, fileName)
		return mpxarPath, "", false, nil

	case strings.HasSuffix(fileName, ".mpxar.didx"):
		mpxarPath = filepath.Join(groupDir, fileName)
		ppxarName := strings.TrimSuffix(fileName, ".mpxar.didx") + ".ppxar.didx"
		ppxarPath = filepath.Join(groupDir, ppxarName)
		if _, err := os.Stat(ppxarPath); err != nil {
			return "", "", false, fmt.Errorf("payload index not found: %s", ppxarPath)
		}
		return mpxarPath, ppxarPath, true, nil

	case strings.HasSuffix(fileName, ".ppxar.didx"):
		ppxarPath = filepath.Join(groupDir, fileName)
		mpxarName := strings.TrimSuffix(fileName, ".ppxar.didx") + ".mpxar.didx"
		mpxarPath = filepath.Join(groupDir, mpxarName)
		if _, err := os.Stat(mpxarPath); err != nil {
			return "", "", false, fmt.Errorf("metadata index not found: %s", mpxarPath)
		}
		return mpxarPath, ppxarPath, true, nil
	}

	return "", "", false, fmt.Errorf("unexpected file-name format")
}
