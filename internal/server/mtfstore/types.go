package mtfstore

import (
	"database/sql"
	"os"
	"strings"
)

type Changer struct {
	Name      string `json:"name"`
	Device    string `json:"device"`
	Comment   string `json:"comment"`
	CreatedAt int64  `json:"created_at"`
}

type Drive struct {
	Name       string `json:"name"`
	Device     string `json:"device"`
	Changer    string `json:"changer"`
	DriveIndex int    `json:"drive_index"`
	Comment    string `json:"comment"`
	CreatedAt  int64  `json:"created_at"`
}

type Cartridge struct {
	Barcode         string `json:"barcode"`
	Label           string `json:"label"`
	MediaFamilyID   int64  `json:"media_family_id"`
	MediaFamilyName string `json:"media_family_name"`
	Sequence        int    `json:"sequence"`
	Role            string `json:"role"`
	CatalogType     int    `json:"catalog_type"`
	IsBkfFile       bool   `json:"is_bkf_file"`
	SourcePath      string `json:"source_path"`
	Volumes         int    `json:"volumes"`
	Directories     int    `json:"directories"`
	Files           int    `json:"files"`
	EmptyFiles      int    `json:"empty_files"`
	FileBytes       int64  `json:"file_bytes"`
	SparseFiles     int    `json:"sparse_files"`
	CompressedFiles int    `json:"compressed_files"`
	EncryptedFiles  int    `json:"encrypted_files"`
	HasCatalog      bool   `json:"has_catalog"`
	CatalogBytes    int64  `json:"catalog_bytes"`
	SetsClosed      int    `json:"sets_closed"`
	Status          string `json:"status"`
	LastScanned     int64  `json:"last_scanned"`
	CreatedAt       int64  `json:"created_at"`
}

type MediaFamily struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	TotalTapes     int    `json:"total_tapes"`
	CartridgeCount int    `json:"cartridge_count"`
	HasCatalog     bool   `json:"has_catalog"`
	DataSetCount   int    `json:"data_set_count"`
	LastScanned    int64  `json:"last_scanned"`
	CreatedAt      int64  `json:"created_at"`
}

type DataSet struct {
	ID             int64           `json:"id"`
	MediaFamilyID  int64           `json:"media_family_id"`
	SetNumber      int             `json:"set_number"`
	Name           string          `json:"name"`
	Description    string          `json:"description"`
	Owner          string          `json:"owner"`
	MachineName    string          `json:"machine_name"`
	WriteTime      int64           `json:"write_time"`
	NumDirectories int             `json:"num_directories"`
	NumFiles       int             `json:"num_files"`
	NumCorrupt     int             `json:"num_corrupt"`
	Size           int64           `json:"size"`
	FirstMediaSeq  int             `json:"first_media_seq"`
	SourceMediaSeq int             `json:"source_media_seq"`
	Volumes        []DataSetVolume `json:"volumes"`
}

type DataSetVolume struct {
	ID              int64  `json:"id"`
	DataSetID       int64  `json:"data_set_id"`
	Device          string `json:"device"`
	VolumeLabel     string `json:"volume_label"`
	MachineName     string `json:"machine_name"`
	MappedNamespace string `json:"mapped_namespace"`
}

type NamespaceMapping struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Priority   int    `json:"priority"`
	MatchRegex string `json:"match_regex"`
	Template   string `json:"template"`
	IsDefault  bool   `json:"is_default"`
	Enabled    bool   `json:"enabled"`
	Comment    string `json:"comment"`
	CreatedAt  int64  `json:"created_at"`
}

type MTFJob struct {
	ID                string     `json:"id"`
	SourceKind        string     `json:"source_kind"`
	SourceRef         string     `json:"source_ref"`
	SourceLabel       string     `json:"source_label"`
	Datastore         string     `json:"datastore"`
	Namespace         string     `json:"namespace"`
	Schedule          string     `json:"schedule"`
	Comment           string     `json:"comment"`
	NotificationMode  string     `json:"notification-mode"`
	Spanning          bool       `json:"spanning"`
	OverwriteMappings bool       `json:"overwrite_mappings"`
	Changer           string     `json:"changer"`
	Drive             string     `json:"drive"`
	Retry             int        `json:"retry"`
	RetryInterval     int        `json:"retry-interval"`
	CurrentPID        string     `json:"current_pid"`
	History           JobHistory `json:"history"`
	CreatedAt         int64      `json:"created_at"`
}

type JobHistory struct {
	LastRunUpid           string    `json:"last-run-upid"`
	LastRunStarttime      int64     `json:"last-run-starttime"`
	LastRunState          string    `json:"last-run-state"`
	LastRunStatus         JobStatus `json:"last-run-status"`
	LastRunEndtime        int64     `json:"last-run-endtime"`
	LastSuccessfulEndtime int64     `json:"last-successful-endtime"`
	LastSuccessfulUpid    string    `json:"last-successful-upid"`
	RetryCount            int       `json:"retry-count"`
	Duration              int64     `json:"duration"`
}

type InventoryRun struct {
	ID          int64  `json:"id"`
	Changer     string `json:"changer"`
	StartedAt   int64  `json:"started_at"`
	CompletedAt int64  `json:"completed_at"`
	Status      string `json:"status"`
	Cartridges  int    `json:"cartridges"`
	Message     string `json:"message"`
}

func ns(s sql.NullString) string {
	if s.Valid {
		return s.String
	}
	return ""
}

func ni64(s sql.NullInt64) int64 {
	if s.Valid {
		return s.Int64
	}
	return 0
}

func ni(s sql.NullInt64) int {
	return int(ni64(s))
}

func nb(s sql.NullInt64) bool {
	return ni64(s) != 0
}

// ResolveTapeDevice converts a SCSI generic device path (-sg) to the
// corresponding non-rewind tape device (-nst). udev by-id paths ending
// in -sg point to SCSI generic devices; the matching tape device has the
// same name with -nst.
func ResolveTapeDevice(path string) string {
	if path == "" {
		return path
	}
	if before, ok := strings.CutSuffix(path, "-sg"); ok {
		nstPath := before + "-nst"
		if _, err := os.Stat(nstPath); err == nil {
			return nstPath
		}
	}
	return path
}
