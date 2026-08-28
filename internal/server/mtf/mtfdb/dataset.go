package mtfdb

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
	SSETPBA        int64           `json:"sset_pba"`
	FirstMediaSeq  int             `json:"first_media_seq"`
	SourceMediaSeq int             `json:"source_media_seq"`
	Volumes        []DataSetVolume `json:"volumes"`
	Tapes          []DataSetTape   `json:"tapes"`
}

type DataSetTape struct {
	ID        int64 `json:"id"`
	DataSetID int64 `json:"data_set_id"`
	MediaSeq  int64 `json:"media_seq"`
	SSETPBA   int64 `json:"sset_pba"`
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
