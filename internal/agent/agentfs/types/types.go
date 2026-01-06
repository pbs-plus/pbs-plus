package types

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
		BackupId   string `cbor:"backup_id"`
		Drive      string `cbor:"drive"`
		SourceMode string `cbor:"source_mode,omitempty"`
		ReadMode   string `cbor:"read_mode,omitempty"`
		Extras     string `cbor:"extras,omitempty"`
	}

	RestoreReq struct {
		RestoreId string `cbor:"restore_id"`
		SrcPath   string `cbor:"src_path"`
		DestPath  string `cbor:"dest_path"`
		Extras    string `cbor:"extras,omitempty"`
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
		Tag   string `cbor:"tag"`
		ID    int32  `cbor:"id"`
		Perms uint8  `cbor:"perms"`
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
