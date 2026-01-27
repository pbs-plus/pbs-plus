package system

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/agent/agentfs/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func FileTree(basePath string, subPath string) (types.FileTreeResp, error) {
	rawPath := filepath.Clean(subPath)
	rawPath = strings.TrimPrefix(rawPath, filepath.VolumeName(rawPath))
	safeRequestedPath := strings.TrimLeft(rawPath, string(filepath.Separator))
	if safeRequestedPath == "." {
		safeRequestedPath = ""
	}

	localFullPath := filepath.Join(basePath, safeRequestedPath)

	syslog.L.Info().
		WithMessage("received filetree request").
		WithField("path", safeRequestedPath).
		WithField("resolved", localFullPath).
		Write()

	entries, err := os.ReadDir(localFullPath)
	if err != nil {
		return types.FileTreeResp{}, err
	}

	var catalog []types.FileTreeEntry
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		virtualItemPath := filepath.Join(safeRequestedPath, entry.Name())
		encodedPath := utils.EncodePath(virtualItemPath)

		item := types.FileTreeEntry{
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
	return types.FileTreeResp{Data: catalog}, nil
}
