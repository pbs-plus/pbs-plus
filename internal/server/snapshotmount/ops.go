//go:build linux

package snapshotmount

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/validate"
)

func ParseMountPoints() ([]string, error) {
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	var mps []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), " - ")
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[0])
		if len(fields) < 5 {
			continue
		}
		mps = append(mps, fields[4])
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return mps, nil
}

func IsMounted(path string) bool {
	mountInfoFile, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	defer func() {
		if err := mountInfoFile.Close(); err != nil {
			log.Error(err, "")
		}
	}()

	scanner := bufio.NewScanner(mountInfoFile)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 5 && fields[4] == path {
			return true
		}
	}
	return false
}

func UnmountPath(mountPoint string) error {
	if err := exec.Command("fusermount3", "-uz", mountPoint).Run(); err == nil {
		return nil
	}
	if err := exec.Command("fusermount", "-uz", mountPoint).Run(); err == nil {
		return nil
	}
	if err := exec.Command("umount", "-f", "-l", mountPoint).Run(); err == nil {
		return nil
	}
	return fmt.Errorf("failed to unmount %s", mountPoint)
}

func RemoveEmptyDirsToBase(path, basePath string) {
	path = filepath.Clean(path)
	basePath = filepath.Clean(basePath)

	for path != basePath && path != "/" && validate.IsPathWithin(basePath, path) {
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) > 0 {
			break
		}
		if err := os.Remove(path); err != nil {
			break
		}
		path = filepath.Dir(path)
	}
}

func removeSessionSockets(s Session) {
	if s.SocketPath == "" {
		return
	}
	for _, suffix := range []string{"", ".monitor", ".log"} {
		if err := os.Remove(s.SocketPath + suffix); err != nil && !os.IsNotExist(err) {
			log.Error(err, "", "socket", s.SocketPath+suffix)
		}
	}
}

func cleanupSessionFiles(s Session) {
	if s.OverlayDir != "" {
		if err := os.RemoveAll(s.OverlayDir); err != nil {
			log.Error(err, "", "overlay", s.OverlayDir)
		}
	}
	removeSessionSockets(s)
	if err := DeleteSession(s.ServiceKey); err != nil {
		log.Error(err, "")
	}
}
