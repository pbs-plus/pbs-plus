package types

import (
	"fmt"
	"net"
	"os"
	"strings"
)

type (
	OpenFileReq struct {
		Path string `cbor:"path"`
		Flag int    `cbor:"flag"`
		Perm int    `cbor:"perm"`
	}

	StatReq struct {
		Path    string `cbor:"path"`
		AclOnly bool   `cbor:"acl_only,omitempty"`
	}

	XAttrReq struct {
		Path    string `cbor:"path"`
		Attr    string `cbor:"attr"`
		AclOnly bool   `cbor:"acl_only,omitempty"`
	}

	FileHandleId uint64

	ReadDirReq struct {
		HandleID FileHandleId `cbor:"handle_id"`
	}

	ReadReq struct {
		HandleID FileHandleId `cbor:"handle_id"`
		Length   int          `cbor:"length"`
	}

	ReadAtReq struct {
		HandleID FileHandleId `cbor:"handle_id"`
		Offset   int64        `cbor:"offset"`
		Length   int          `cbor:"length"`
	}

	CloseReq struct {
		HandleID FileHandleId `cbor:"handle_id"`
	}

	BackupReq struct {
		BackupId   string `cbor:"job_id"`
		Drive      string `cbor:"drive"`
		SourceMode string `cbor:"source_mode,omitempty"`
		ReadMode   string `cbor:"read_mode,omitempty"`
		Extras     string `cbor:"extras,omitempty"`
	}

	RestoreReq struct {
		RestoreId string `cbor:"job_id"`
		SrcPath   string `cbor:"src_path"`
		DestPath  string `cbor:"dest_path"`
		Mode      int    `cbor:"mode"`
		Extras    string `cbor:"extras,omitempty"`
	}

	RestoreCloseReq struct {
		RestoreId string `cbor:"job_id"`
	}

	FileTreeReq struct {
		HostPath string `cbor:"host_path"`
		SubPath  string `cbor:"subpath"`
		Extras   string `cbor:"extras,omitempty"`
	}

	FileTreeEntry struct {
		Filepath string `json:"filepath"`
		Leaf     bool   `json:"leaf"`
		Text     string `json:"text"`
		Type     string `json:"type"`
		Mtime    int64  `json:"mtime,omitempty"`
		Size     int64  `json:"size,omitempty"`
	}

	FileTreeResp struct {
		Data []FileTreeEntry `json:"data"`
	}

	LseekReq struct {
		HandleID FileHandleId `cbor:"handle_id"`
		Offset   int64        `cbor:"offset"`
		Whence   int          `cbor:"whence"`
	}

	TargetStatusReq struct {
		Drive   string `cbor:"drive"`
		Subpath string `cbor:"subpath,omitempty"`
	}

	LseekResp struct {
		NewOffset int64 `cbor:"new_offset"`
	}

	WinACL struct {
		SID        string `cbor:"sid"`
		AccessMask uint32 `cbor:"access_mask"`
		Type       uint8  `cbor:"type"`
		Flags      uint8  `cbor:"flags"`
	}

	PosixACL struct {
		Tag       string `cbor:"tag"`
		ID        int32  `cbor:"id"`
		Perms     uint8  `cbor:"perms"`
		IsDefault bool   `cbor:"is_default"`
	}

	AgentFileInfo struct {
		Name           string          `cbor:"name"`
		Size           int64           `cbor:"size"`
		Mode           uint32          `cbor:"mode"`
		ModTime        int64           `cbor:"mod_time"`
		IsDir          bool            `cbor:"is_dir"`
		Blocks         uint64          `cbor:"blocks,omitempty"`
		CreationTime   int64           `cbor:"creation_time,omitempty"`
		LastAccessTime int64           `cbor:"last_access_time,omitempty"`
		LastWriteTime  int64           `cbor:"last_write_time,omitempty"`
		FileAttributes map[string]bool `cbor:"file_attributes,omitempty"`
		Owner          string          `cbor:"owner,omitempty"`
		Group          string          `cbor:"group,omitempty"`
		WinACLs        []WinACL        `cbor:"win_acls,omitempty"`
		PosixACLs      []PosixACL      `cbor:"posix_acls,omitempty"`
	}

	StatFS struct {
		Bsize   uint64 `cbor:"bsize"`
		Blocks  uint64 `cbor:"blocks"`
		Bfree   uint64 `cbor:"bfree"`
		Bavail  uint64 `cbor:"bavail"`
		Files   uint64 `cbor:"files"`
		Ffree   uint64 `cbor:"ffree"`
		NameLen uint64 `cbor:"name_len"`
	}

	ReadDirEntries []AgentFileInfo
)

type DriveInfo struct {
	Letter     string `json:"letter"`
	Type       string `json:"type"`
	VolumeName string `json:"volume_name"`
	FileSystem string `json:"filesystem"`
	TotalBytes uint64 `json:"total_bytes"`
	UsedBytes  uint64 `json:"used_bytes"`
	FreeBytes  uint64 `json:"free_bytes"`
	Total      string `json:"total"`
	Used       string `json:"used"`
	Free       string `json:"free"`
}

func HumanizeBytes(bytes uint64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := unit, 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	var unitSymbol string
	switch exp {
	case 0:
		unitSymbol = "KB"
	case 1:
		unitSymbol = "MB"
	case 2:
		unitSymbol = "GB"
	case 3:
		unitSymbol = "TB"
	case 4:
		unitSymbol = "PB"
	default:
		unitSymbol = "??"
	}
	return fmt.Sprintf("%.2f %s", float64(bytes)/float64(div), unitSymbol)
}

// GetAgentHostname returns the hostname from PBS_PLUS_HOSTNAME env or falls
// back to the OS hostname.
func GetAgentHostname() (string, error) {
	host := os.Getenv("PBS_PLUS_HOSTNAME")
	if host == "" {
		return os.Hostname()
	}
	if err := ValidateHostname(host); err != nil {
		return "", err
	}
	return host, nil
}

// ValidateHostname checks that a hostname is valid.
func ValidateHostname(host string) error {
	if host == "" {
		return fmt.Errorf("hostname cannot be empty")
	}
	if len(host) > 253 {
		return fmt.Errorf("hostname too long (%d chars)", len(host))
	}
	if ip := net.ParseIP(host); ip != nil {
		return nil
	}
	for part := range strings.SplitSeq(host, ".") {
		if part == "" {
			return fmt.Errorf("hostname segment cannot be empty")
		}
		if len(part) > 63 {
			return fmt.Errorf("hostname segment too long: %s", part)
		}
		for _, c := range part {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-') {
				return fmt.Errorf("hostname segment %q contains invalid character %q", part, string(c))
			}
		}
	}
	return nil
}
