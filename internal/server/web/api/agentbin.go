//go:build linux

package api

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
)

//go:embed agentbin
var agentBinFS embed.FS

var embedModTime = time.Now()

func embeddedVersion() string {
	data, err := fs.ReadFile(agentBinFS, "agentbin/VERSION")
	if err != nil {
		return ""
	}
	v := strings.TrimSpace(string(data))
	if v == "" || v == "dev" {
		return ""
	}
	return v
}

func embeddedFile(name string) []byte {
	data, err := fs.ReadFile(agentBinFS, "agentbin/"+name)
	if err != nil {
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	return data
}

func embeddedBinary(osName, arch string) []byte {
	name := fmt.Sprintf("pbs-plus-agent-%s-%s-%s", embeddedVersion(), osName, arch)
	if osName == "windows" {
		name += ".exe"
	}
	return embeddedFile(name)
}

func embeddedChecksum(osName, arch string) []byte {
	bin := embeddedBinary(osName, arch)
	if bin == nil {
		return nil
	}
	hash := sha256.Sum256(bin)
	return []byte(hex.EncodeToString(hash[:]))
}

func serveEmbeddedContent(w http.ResponseWriter, r *http.Request, name string) bool {
	data := embeddedFile(name)
	if data == nil {
		return false
	}

	w.Header().Set("X-Embedded", "true")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))

	http.ServeContent(w, r, name, embedModTime, bytes.NewReader(data))
	return true
}

func serveEmbeddedBinary(w http.ResponseWriter, r *http.Request, version, osName, arch string) bool {
	ev := embeddedVersion()
	if ev == "" || ev != version {
		return false
	}

	name := fmt.Sprintf("pbs-plus-agent-%s-%s-%s", version, osName, arch)
	if osName == "windows" {
		name += ".exe"
	}

	if embeddedFile(name) == nil {
		return false
	}

	sigName := name + ".sig"
	ecdsaSigName := name + ".ecdsa-sig"
	if embeddedFile(sigName) == nil || embeddedFile(ecdsaSigName) == nil {
		return false
	}

	return serveEmbeddedContent(w, r, name)
}

func serveEmbeddedSignature(w http.ResponseWriter, r *http.Request, version, osName, arch, suffix string) bool {
	ev := embeddedVersion()
	if ev == "" || ev != version {
		return false
	}

	if suffix == ".sha256" {
		data := embeddedChecksum(osName, arch)
		if data == nil {
			return false
		}
		w.Header().Set("X-Embedded", "true")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		if _, err := w.Write(data); err != nil {
			syslog.L.Error(err).Write()
		}
		return true
	}

	name := fmt.Sprintf("pbs-plus-agent-%s-%s-%s%s%s", version, osName, arch, windowsExt(osName), suffix)
	return serveEmbeddedContent(w, r, name)
}

func windowsExt(osName string) string {
	if osName == "windows" {
		return ".exe"
	}
	return ""
}
