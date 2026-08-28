package filetree

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/fswire"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func Read(basePath string, subPath string) (fswire.FileTreeResp, error) {
	rawPath := filepath.Clean(subPath)
	rawPath = strings.TrimPrefix(rawPath, filepath.VolumeName(rawPath))
	safeRequestedPath := strings.TrimLeft(rawPath, string(filepath.Separator))
	if safeRequestedPath == "." {
		safeRequestedPath = ""
	}

	localFullPath := filepath.Join(basePath, safeRequestedPath)
	log.Info("received filetree request",

		"resolved", localFullPath, "path", safeRequestedPath)

	entries, err := os.ReadDir(localFullPath)
	if err != nil {
		return fswire.FileTreeResp{}, err
	}

	var catalog []fswire.FileTreeEntry
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		virtualItemPath := filepath.Join(safeRequestedPath, entry.Name())
		encodedPath := encodePath(virtualItemPath)

		item := fswire.FileTreeEntry{
			Filepath: encodedPath,
			Text:     entry.Name(),
			Leaf:     !entry.IsDir(),
			Type:     "f",
		}

		if entry.IsDir() {
			item.Type = "d"
		} else {
			item.Mtime = info.ModTime().Unix()
			item.Size = info.Size()
		}

		catalog = append(catalog, item)
	}
	return fswire.FileTreeResp{Data: catalog}, nil
}
