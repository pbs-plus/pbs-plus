//go:build linux

package snapshotmount

import (
	"bufio"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/log"
	"github.com/pbs-plus/pbs-plus/internal/server/vfs"
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

	return parseMountPoints(f)
}

func parseMountPoints(r io.Reader) ([]string, error) {
	var mps []string
	unescape := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		parts := strings.Split(sc.Text(), " - ")
		if len(parts) != 2 {
			continue
		}
		fields := strings.Fields(parts[0])
		if len(fields) < 5 {
			continue
		}
		mps = append(mps, unescape.Replace(fields[4]))
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return mps, nil
}

func IsMounted(path string) bool {
	resolved, rerr := filepath.EvalSymlinks(path)
	mountPoints, err := ParseMountPoints()
	if err != nil {
		return false
	}
	for _, mountPoint := range mountPoints {
		if mountPoint == path || (rerr == nil && mountPoint == resolved) {
			return true
		}
	}
	return false
}

func UnmountPath(mountPoint string) error {
	return vfs.ForceUnmount(context.Background(), mountPoint)
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
