//go:build linux

package types

import (
	"strings"

	s3url "github.com/pbs-plus/pbs-plus/internal/backend/vfs/s3/url"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func (t *TargetPath) IsAgent() bool {
	if strings.HasPrefix(string(*t), "agent://") {
		return true
	}

	return false
}

func (t *TargetPath) IsS3() bool {
	if _, err := s3url.Parse(string(*t)); err == nil {
		return true
	}

	return false
}

func (t *TargetPath) IsLocal() bool {
	if t.IsAgent() || t.IsS3() {
		return false
	}

	return utils.IsValid(string(*t))
}

func (t *TargetPath) GetPathInfo() PathInfo {
	if strings.HasPrefix(string(*t), "agent://") {
		trimmed := string(*t)[8:]
		lastSlash := strings.LastIndex(trimmed, "/")

		hostPath := ""
		if lastSlash != -1 {
			resource := trimmed[lastSlash+1:]

			if resource == "root" {
				hostPath = "/"
			} else if len(resource) == 1 {
				hostPath = resource + ":\\"
			} else {
				hostPath = resource
			}
		}

		return PathInfo{
			Type:     TargetTypeAgent,
			RawPath:  string(*t),
			HostPath: hostPath,
		}
	}

	if _, err := s3url.Parse(string(*t)); err == nil {
		return PathInfo{
			Type:    TargetTypeS3,
			RawPath: string(*t),
		}
	}

	return PathInfo{
		Type:    TargetTypeLocal,
		RawPath: string(*t),
	}
}
